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
  Success,
  Error
};

enum RowSetFlag {
  InsertedRow,
  DeletedRow,
  End,
  Item
};

bool trouble = false;
int globalSocket = -1;

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

bool
flush(int socket, Buffer* buffer)
{
  int r = write(socket, buffer->array, buffer->position);
  if (r != buffer->position) {
    perror("unable to write");
    trouble = true;
    return false;
  }
  buffer->position = 0;
  return true;
}

int
writeBytes(int socket, Buffer* buffer, const char* src, int count)
{
  int total = 0;
  while (total < count) {
    if (buffer->limit == buffer->position) {
      if (not flush(socket, buffer)) {
        return -1;
      }
    }

    int c = min(count - total, buffer->limit - buffer->position);
    memcpy(buffer->array + buffer->position, src + total, c);
    buffer->position += c;
    total += c;
  }
  return total;
}

bool
writeByte(int socket, Buffer* buffer, int v)
{
  char src[] = { v };
  return writeBytes(socket, buffer, src, 1) == 1;
}

bool
writeInteger(int socket, Buffer* buffer, int v)
{
  char src[] = { (v >> 24) & 0xFF,
                 (v >> 16) & 0xFF,
                 (v >>  8) & 0xFF,
                 (v      ) & 0xFF };
  return writeBytes(socket, buffer, src, 4) == 4;
}

bool
writeString(int socket, Buffer* buffer, const char* s, int length)
{
  if (writeInteger(socket, buffer, length)) {
    return writeBytes(socket, buffer, s, length) == length;
  } else {
    return false;
  }
}

int
readBytes(int socket, Buffer* buffer, char* dst, int count)
{
  int total = 0;
  while (total < count) {
    if (buffer->limit == buffer->position) {
      buffer->position = 0;
      int r = read(socket, buffer->array, buffer->capacity);
      if (r == 0) {
        fprintf(stderr, "unexpected end of stream from server\n");
        trouble = true;
        buffer->limit = 0;
        return -1;
      } else if (r < 0) {
        perror("unable to read");
        trouble = true;
        buffer->limit = 0;
        return -1;
      }
      buffer->limit = r;
    }

    int c = min(count - total, buffer->limit - buffer->position);
    memcpy(dst + total, buffer->array + buffer->position, c);
    buffer->position += c;
    total += c;
  }
  return total;
}

int
readByte(int socket, Buffer* buffer)
{
  char dst[1];
  int r = readBytes(socket, buffer, dst, 1);
  return r < 1 ? -1 : dst[0];
}

int
readInteger(int socket, Buffer* buffer)
{
  char dst[4];
  int r = readBytes(socket, buffer, dst, 4);
  return r < 4 ? -1 : (  (((int) dst[0]) << 24)
                       | (((int) dst[1]) << 16)
                       | (((int) dst[2]) <<  8)
                       | (((int) dst[3])      ));
}

char*
readString(int socket, Buffer* buffer)
{
  int length = readInteger(socket, buffer);
  if (length < 0) {
    return 0;
  }

  char* string = static_cast<char*>(malloc(length + 1));
  if (string == 0) {
    fprintf(stderr, "unable to allocate memory\n");
    trouble = true;
    return 0;
  }

  int count = readBytes(socket, buffer, string, length);
  if (count != length) {
    free(string);
    return 0;
  }

  return string;
}

char**
complete(const char*, int, int end)
{
  if (trouble) {
    return 0;
  }

  Buffer buffer(8 * 1024);
  if (buffer.array == 0) {
    fprintf(stderr, "unable to allocate memory\n");
    return 0;
  }

  buffer.limit = buffer.capacity;

  int socket = globalSocket;

  if (not writeByte(socket, &buffer, Complete)) {
    return 0;
  }

  if (not writeString(socket, &buffer, rl_line_buffer, end)) {
    return 0;
  }

  if (not flush(socket, &buffer)) {
    return 0;
  }

  buffer.limit = 0;

  int result = readByte(socket, &buffer);
  switch (result) {
  case -1:
    break;

  case Success: {
    int count = readInteger(socket, &buffer);
    if (count < 0) {
      return 0;
    }
    
    char** result = static_cast<char**>(malloc(sizeof(char*) * count));
    if (result == 0) {
      fprintf(stderr, "unable to allocate memory\n");
      return 0;
    }

    for (int i = 0; i < count; ++i) {
      result[i] = readString(socket, &buffer);
      if (result[i] == 0) {
        free(result);
        return 0;
      }
    }

    return result;
  } break;

  case Error: {
    char* message = readString(socket, &buffer);
    if (message == 0) {
      return 0;
    }
   
    free(message);
  } break;

  default:
    fprintf(stderr, "unexpected result from server: %d\n", result);
    trouble = true;
    break;
  }

  return 0;
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
execute(int socket, const char* command)
{
  if (trouble) {
    return;
  }

  Buffer buffer(8 * 1024);
  if (buffer.array == 0) {
    fprintf(stderr, "unable to allocate memory\n");
    return;
  }

  buffer.limit = buffer.capacity;

  if (not writeByte(socket, &buffer, Execute)) {
    return;
  }

  if (not writeString(socket, &buffer, command, strlen(command))) {
    return;
  }

  if (not flush(socket, &buffer)) {
    return;
  }

  buffer.limit = 0;

  int result = readByte(socket, &buffer);
  switch (result) {
  case -1:
    break;

  case Success: {
    char* message = readString(socket, &buffer);
    if (message == 0) {
      return;
    }

    fprintf(stdout, "%s\n", message);
   
    free(message);
  } break;

  case RowSet: {
    bool done = false;
    while (not done) {
      int flag = readByte(socket, &buffer);
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
        char* item = readString(socket, &buffer);
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
        fprintf(stderr, "unexpected flag from server: %d\n", flag);
        trouble = true;
        return;
      }
    }
  } break;

  case Error: {
    char* message = readString(socket, &buffer);
    if (message == 0) {
      return;
    }

    fprintf(stderr, "error: %s\n", message);
   
    free(message);
  } break;

  default:
    fprintf(stderr, "unexpected result from server: %d\n", result);
    trouble = true;
    break;
  }
}

} // namespace

int
main(int, const char**)
{
  int socket = connect("localhost", 8017);
  if (socket < 0) {
    return -1;
  }

  globalSocket = socket;

  rl_readline_name = "DBMSClient";

  rl_attempted_completion_function = complete;

  while (not trouble) {
    char* line = readline("> ");

    if (line == 0) {
      break;
    }

    char* s = removeEdgeWhitespace(line);
    if (*s) {
      add_history(s);

      execute(socket, s);
    }

    free(line);
  }

  close(socket);

  return 0;
}
