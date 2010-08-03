#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>

#include <readline/readline.h>
#include <readline/history.h>

namespace {

enum Request {
  Execute,
  Complete
};

enum Response {
  RowSet,
  NewDatabase,
  Success,
  Error
};

enum RowSetFlag {
  InsertedRow,
  DeletedRow,
  End,
  Item
};

class Buffer {
 public:
  Buffer(int capacity):
    array(static_cast<char*>(malloc(capacity))),
    capacity(capacity),
    position(0),
    limit(0)
  { }

  ~Buffer() {
    if (array) free(array);
  }

  char* array;
  int capacity;
  int position;
  int limit;
};

class Context {
 public:
  Context():
    buffer(8 * 1024),
    databaseName(0),
    socket(-1),
    completionCount(-1),
    trouble(false),
    exit(false)
  { }

  ~Context() {
    if (databaseName) {
      free(databaseName);
    }
  }

  Buffer buffer;
  char* databaseName;
  int socket;
  int completionCount;
  bool trouble;
  bool exit;
};

Context* globalContext = 0;

int
min(int a, int b)
{
  return a > b ? b : a;
}

int
connect(const char* host, int port)
{
  addrinfo hints;
  memset(&hints, 0, sizeof(addrinfo));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  addrinfo* result;
  int r = getaddrinfo(host, 0, &hints, &result);
  if (r != 0) {
    fprintf(stderr, "unable to resolve localhost: %s\n", gai_strerror(r));
    return -1;
  }

  sockaddr_in address;
  memset(&address, 0, sizeof(sockaddr_in));
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr = reinterpret_cast<sockaddr_in*>(result->ai_addr)->sin_addr;

  freeaddrinfo(result);

  int socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (socket < 0) {
    perror("unable to open socket");
    return -1;
  }

  r = connect(socket, reinterpret_cast<sockaddr*>(&address),
              sizeof(sockaddr_in));
  if (r != 0) {
    perror("unable to connect");
    return -1;
  }

  return socket;
}

bool
flush(Context* context)
{
  int r = write
    (context->socket, context->buffer.array, context->buffer.position);
  if (r != context->buffer.position) {
    perror("\nunable to write");
    context->trouble = true;
    return false;
  }
  context->buffer.position = 0;
  return true;
}

int
writeBytes(Context* context, const char* src, int count)
{
  int total = 0;
  while (total < count) {
    if (context->buffer.limit == context->buffer.position) {
      if (not flush(context)) {
        return -1;
      }
    }

    int c = min
      (count - total, context->buffer.limit - context->buffer.position);
    memcpy(context->buffer.array + context->buffer.position, src + total, c);
    context->buffer.position += c;
    total += c;
  }
  return total;
}

bool
writeByte(Context* context, int v)
{
  char src[] = { v };
  return writeBytes(context, src, 1) == 1;
}

bool
writeInteger(Context* context, int v)
{
  char src[] = { (v >> 24) & 0xFF,
                 (v >> 16) & 0xFF,
                 (v >>  8) & 0xFF,
                 (v      ) & 0xFF };
  return writeBytes(context, src, 4) == 4;
}

bool
writeString(Context* context, const char* s, int length)
{
  if (writeInteger(context, length)) {
    return writeBytes(context, s, length) == length;
  } else {
    return false;
  }
}

int
readBytes(Context* context, char* dst, int count)
{
  int total = 0;
  while (total < count) {
    if (context->buffer.limit == context->buffer.position) {
      context->buffer.position = 0;
      int r = read
        (context->socket, context->buffer.array, context->buffer.capacity);
      if (r == 0) {
        fprintf(stderr, "\nunexpected end of stream from server\n");
        context->trouble = true;
        context->buffer.limit = 0;
        return -1;
      } else if (r < 0) {
        perror("\nunable to read");
        context->trouble = true;
        context->buffer.limit = 0;
        return -1;
      }
      context->buffer.limit = r;
    }

    int c = min
      (count - total, context->buffer.limit - context->buffer.position);
    memcpy(dst + total, context->buffer.array + context->buffer.position, c);
    context->buffer.position += c;
    total += c;
  }
  return total;
}

int
readByte(Context* context)
{
  char dst[1];
  int r = readBytes(context, dst, 1);
  return r < 1 ? -1 : dst[0];
}

int
readInteger(Context* context)
{
  char dst[4];
  int r = readBytes(context, dst, 4);
  return r < 4 ? -1 : (  (((int) dst[0]) << 24)
                       | (((int) dst[1]) << 16)
                       | (((int) dst[2]) <<  8)
                       | (((int) dst[3])      ));
}

char*
readString(Context* context)
{
  int length = readInteger(context);
  if (length < 0) {
    return 0;
  }

  char* string = static_cast<char*>(malloc(length + 1));
  if (string == 0) {
    fprintf(stderr, "\nunable to allocate memory\n");
    context->trouble = true;
    return 0;
  }

  int count = readBytes(context, string, length);
  if (count != length) {
    free(string);
    return 0;
  }

  string[length] = 0;

  return string;
}

int
startCompletion(Context* context, const char* text)
{
  context->buffer.position = 0;
  context->buffer.limit = context->buffer.capacity;

  if (not writeByte(context, Complete)) {
    return -1;
  }

  if (not writeString(context, text, strlen(text))) {
    return -1;
  }

  if (not flush(context)) {
    return -1;
  }

  context->buffer.limit = 0;

  int result = readByte(context);
  switch (result) {
  case -1:
    break;

  case Success: {
    return readInteger(context);
  } break;

  case Error: {
    char* message = readString(context);
    if (message == 0) {
      return -1;
    }
   
    free(message);
  } break;

  default:
    fprintf(stderr, "\nunexpected result from server: %d\n", result);
    context->trouble = true;
    break;
  }

  return -1;
}

char*
completionGenerator(const char*, int state)
{
  Context* context = globalContext;

  if (context->trouble) {
    return 0;
  }

  if (state == 0) {
    context->completionCount = startCompletion(context, rl_line_buffer);
  }
  
  if (context->completionCount <= 0) {
    return 0;
  } else {
    -- context->completionCount;

    return readString(context);
  }
}

char*
removeEdgeWhitespace(char* s)
{
  while (isspace(*s)) ++s;

  if (*s == 0) return s;

  char* p = s;
  char* lastNonSpace = p;
  while (*p) {
    if (not isspace(*p)) {
      lastNonSpace = p;
    }
    ++ p;
  }

  lastNonSpace[1] = 0;

  return s;
}

void
execute(Context* context, const char* command)
{
  if (strcmp(command, "exit") == 0
      || strcmp(command, "quit") == 0)
  {
    context->exit = true;
    return;
  }

  if (context->trouble) {
    return;
  }

  context->buffer.position = 0;
  context->buffer.limit = context->buffer.capacity;

  if (not writeByte(context, Execute)) {
    return;
  }

  if (not writeString(context, command, strlen(command))) {
    return;
  }

  if (not flush(context)) {
    return;
  }

  context->buffer.limit = 0;

  int result = readByte(context);
  switch (result) {
  case -1:
    break;

  case NewDatabase: {
    if (context->databaseName) {
      free(context->databaseName);
    }

    context->databaseName = readString(context);
    if (context->databaseName == 0) {
      return;
    }

    char* message = readString(context);
    if (message == 0) {
      return;
    }

    fprintf(stdout, "%s\n", message);
   
    free(message);
  } break;

  case Success: {
    char* message = readString(context);
    if (message == 0) {
      return;
    }

    fprintf(stdout, "%s\n", message);
   
    free(message);
  } break;

  case RowSet: {
    bool done = false;
    while (not done) {
      int flag = readByte(context);
      switch (flag) {
      case -1:
        return;

      case InsertedRow:
        fprintf(stdout, "\n inserted:");
        break;

      case DeletedRow:
        fprintf(stdout, "\n  deleted:");
        break;

      case Item: {
        char* item = readString(context);
        if (item == 0) {
          return;
        }
        
        fprintf(stdout, " %s", item);

        free(item);
      } break;

      case End:
        done = true;
        fprintf(stdout, "\n");
        break;

      default:
        fprintf(stderr, "\nunexpected flag from server: %d\n", flag);
        context->trouble = true;
        return;
      }
    }
  } break;

  case Error: {
    char* message = readString(context);
    if (message == 0) {
      return;
    }

    fprintf(stderr, "error: %s\n", message);
   
    free(message);
  } break;

  default:
    fprintf(stderr, "\nunexpected result from server: %d\n", result);
    context->trouble = true;
    break;
  }
}

} // namespace

int
main(int, const char**)
{
  Context context;
  globalContext = &context;

  if (context.buffer.array == 0) {
    fprintf(stderr, "\nunable to allocate memory\n");
    return -1;
  }

  context.socket = connect("localhost", 8017);
  if (context.socket < 0) {
    return -1;
  }

  rl_readline_name = "DBMSClient";

  rl_completion_entry_function = completionGenerator;

  while (not (context.trouble || context.exit)) {
    char* line;
    if (context.databaseName) {
      int length = strlen(context.databaseName) + 4;
      char* prompt = static_cast<char*>(malloc(length));
      if (prompt == 0) {
        fprintf(stderr, "\nunable to allocate memory\n");
        return -1;
      }

      snprintf(prompt, length, "%s > ", context.databaseName);
      line = readline(prompt);
      free(prompt);
    } else {
      line = readline("> ");
    }

    if (line == 0) {
      fprintf(stdout, "\n");
      break;
    }

    char* s = removeEdgeWhitespace(line);
    if (*s) {
      add_history(s);

      execute(&context, s);
      fprintf(stdout, "\n");
    }

    free(line);
  }

  close(context.socket);

  return context.trouble ? -1 : 0;
}
