#include <netinet/in.h>
#include <stdlib.h>
#include <tcp_server.h>

void handle_received_data(int fd, char *buffer, int read, int buffer_size) {
  plog("message:%s", buffer);
  if (read >= 2 && buffer[0] == ':' && buffer[1] == 'q') {
    plog("quitting\n");
    exit(1);
  }
  // Send message bach to the client
  send(fd, buffer, read, 0);
}

int main() {
	start();
}
