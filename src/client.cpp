/* Copyright (c) 2010-2012, Revori Contributors

   Permission to use, copy, modify, and/or distribute this software
   for any purpose with or without fee is hereby granted, provided
   that the above copyright notice and this permission notice appear
   in all copies. */

#include <stdlib.h>
#include <stdio.h>
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
  CopySuccess,
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
    exit(false),
    copying(false)
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
  bool copying;
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
    fprintf(stderr, "unable to resolve host %s: %s\n", host, gai_strerror(r));
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
  char src[] = { static_cast<char>(v) };
  return writeBytes(context, src, 1) == 1;
}

bool
writeInteger(Context* context, int v)
{
  char src[] = { static_cast<char>((v >> 24) & 0xFF),
                 static_cast<char>((v >> 16) & 0xFF),
                 static_cast<char>((v >>  8) & 0xFF),
                 static_cast<char>((v      ) & 0xFF) };
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
  return r < 4 ? -1 : (((dst[0] & 0xFF) << 24) |
                       ((dst[1] & 0xFF) << 16) |
                       ((dst[2] & 0xFF) <<  8) |
                       ((dst[3] & 0xFF)      ));
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

  if (not context->copying) {
    context->buffer.position = 0;
    context->buffer.limit = context->buffer.capacity;
  }

  if (not writeByte(context, Execute)) {
    return;
  }

  if (not writeString(context, command, strlen(command))) {
    return;
  }

  if (context->copying && strcmp(command, "\\.") == 0) {
    if (not flush(context)) {
      return;
    }
    context->copying = false;
  }

  if (context->copying) {
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

  case CopySuccess: {
    char* message = readString(context);
    if (message == 0) {
      return;
    }

    fprintf(stdout, "%s\n", message);
   
    free(message);

    context->buffer.position = 0;
    context->buffer.limit = context->buffer.capacity;

    context->copying = true;
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
    bool sawRow = false;
    while (not done) {
      int flag = readByte(context);
      switch (flag) {
      case -1:
        return;

      case InsertedRow:
        sawRow = true;
        fprintf(stdout, "\n inserted:");
        break;

      case DeletedRow:
        sawRow = true;
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
        if (not sawRow) {
          fprintf(stdout, "\n no matching rows found");
        }
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

void
usage(const char* name)
{
  fprintf(stderr, "usage: %s [--batch] <hostname> <port> [<database>]\n",
          name);  
}

} // namespace

int
main(int argumentCount, const char** arguments)
{
  bool interactive = true;
  const char* hostname = 0;
  int port = -1;
  const char* database = 0;
  for (int i = 1; i < argumentCount; ++i) {
    if (strcmp("--batch", arguments[i]) == 0) {
      interactive = false;
    } else if (hostname == 0) {
      hostname = arguments[i];
    } else if (port == -1) {
      port = atoi(arguments[i]);
    } else if (database == 0) {
      database = arguments[i];
    } else {
      usage(arguments[0]);
      return -1;
    }
  }

  if (hostname == 0 or port == -1) {
    usage(arguments[0]);
    return -1;
  }

  Context context;
  globalContext = &context;

  if (context.buffer.array == 0) {
    fprintf(stderr, "\nunable to allocate memory\n");
    return -1;
  }

  context.socket = connect(hostname, port);
  if (context.socket < 0) {
    return -1;
  }

  if (database) {
    const int BufferSize = 256;
    char buffer[BufferSize];
    snprintf(buffer, BufferSize, "use database %s", database);
    execute(&context, buffer);

    if (context.trouble) {
      return -1;
    }
  }

  if (interactive) {
    fprintf(stdout, "\nWelcome to the DBMS SQL client interface.  "
            "Type \"help\" to get started.\n");

    rl_readline_name = "DBMSClient";

    rl_completion_entry_function = completionGenerator;

    while (not (context.trouble or context.exit)) {
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

      if (context.copying) {
        execute(&context, line);
      } else {
        char* s = removeEdgeWhitespace(line);
        if (*s) {
          add_history(s);

          execute(&context, s);
          fprintf(stdout, "\n");
        }
      }

      free(line);
    }
  } else {
    const int BufferSize = 8096;
    char buffer[BufferSize];
    int i = 0;

    while (true) {
      int c = fgetc(stdin);
      switch (c) {
      case EOF:
        return 0;

      case '\n':
        buffer[i] = 0;
        execute(&context, buffer);
        i = 0;
        break;

      default:
        if (i < BufferSize - 1) {
          buffer[i++] = c;
        } else {
          fprintf(stderr, "buffer overflow\n");
          return -1;
        }
        break;
      }
    }
  }

  close(context.socket);

  return context.trouble ? -1 : 0;
}
