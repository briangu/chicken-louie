// http://codefundas.blogspot.com/2010/09/create-tcp-echo-server-using-libev.html
#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <unistd.h>
#include <strings.h>
#include <ev.h>
#include <fcntl.h>
#include <errno.h>

#define PORT_NO 3033
#define BUFFER_SIZE 1024
#define LISTEN_QUEUE_LENGTH 16*1024

int total_clients = 0;  // Total number of connected clients

typedef struct ev_fork_child {
  ev_child child;
  int sd;
} ev_fork_child;

void accept_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);
void child_cb(EV_P_ ev_child *w, int revents);

void handle_received_data(char *buffer, int read, int buffer_size) {
    // printf("message:%s", buffer);
    if (buffer[0] == 'q') {
      printf("quitting\n");
      exit(1);
    } 
}

int make_socket_nonblocking(int fd)
{
    // non blocking (fix for windows)
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        printf("make_socket_nonblocking: fcntl(F_GETFL) failed, errno: %s\n", strerror(errno));
        return -1;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        printf("make_socket_nonblocking: fcntl(F_SETFL) failed, errno: %s\n", strerror(errno));
        return -1;
    }
    return 1;
}

pid_t create_child_process(int sd) {
  printf("attempting to create child process\n");
  pid_t pid = fork();
  printf("pid_1 = %d\n", pid);
  if (pid == 0) {
    // child process
    // Initialize and start a watcher to accepts client requests
    struct ev_loop *loop = EV_DEFAULT;
    struct ev_io w_accept;
    ev_io_init(&w_accept, accept_cb, sd, EV_READ);
    ev_io_start(loop, &w_accept);

    while (1) {
      ev_loop(loop, 0);
    }
  }
  printf("pid_2 = %d\n", pid);

  return pid;
}

void watch_child(struct ev_loop *loop, int sd, pid_t pid) {
  printf("watching child %d\n", pid);
  ev_fork_child *cw = malloc(sizeof(ev_fork_child));
  cw->sd = sd;
  ev_child_init (&cw->child, child_cb, pid, 0);
  ev_child_start (EV_DEFAULT_ (ev_child *)cw);
}

void child_cb(EV_P_ ev_child *ec, int revents) {
  printf("child_cb\n");

  ev_fork_child *fork_child = (ev_fork_child *)ec;
  ev_child *w = &fork_child->child;

  ev_child_stop (EV_A_ w);
  printf ("child process %d exited with status %x\n", w->rpid, w->rstatus);

  int sd = fork_child->sd;
  free(fork_child);

  pid_t pid = create_child_process(sd);
  if (pid > 0) {
    printf ("created new child process %d\n", pid);
    watch_child(loop, sd, pid);
  } else {
    // failure to create child process
    printf("failed to create child process: %d\n", pid);
  }
}

/* Read client message */
void read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
  char buffer[BUFFER_SIZE];
  ssize_t read;

  if(EV_ERROR & revents) {
    printf("got invalid event");
    return;
  }

  // Receive message from client socket
  read = recv(watcher->fd, buffer, BUFFER_SIZE, 0);
  if (read > 0) {
    handle_received_data(buffer, read, BUFFER_SIZE);
  } else if(read < 0) {
    printf("read error");
    return;
  } else if(read == 0) {
    // Stop and free watcher if client socket is closing
    ev_io_stop(loop, watcher);
    free(watcher);
    printf("peer might closing");
    total_clients --; // Decrement total_clients count
    printf("%d client(s) connected.\n", total_clients);
    return;
  }

  // Send message bach to the client
  send(watcher->fd, buffer, read, 0);
  bzero(buffer, read);
}

/* Accept client requests */
void accept_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
  struct sockaddr_in client_addr;
  socklen_t client_len = sizeof(client_addr);
  int client_sd;
  struct ev_io *w_client = (struct ev_io*) malloc (sizeof(struct ev_io));

  if(EV_ERROR & revents) {
    printf("got invalid event");
    return;
  }

// Accept client request
  client_sd = accept(watcher->fd, (struct sockaddr *)&client_addr, &client_len);
  if (client_sd < 0) {
    printf("accept error");
    return;
  }

  if (make_socket_nonblocking(client_sd) == -1) { 
    printf("failed to set accepted socket to nonblocking mode");
    return;
  }

  total_clients ++; // Increment total_clients count
  printf("Successfully connected with client.\n");
  printf("%d client(s) connected.\n", total_clients);

  // Initialize and start watcher to read client requests
  ev_io_init(w_client, read_cb, client_sd, EV_READ);
  ev_io_start(loop, w_client);
}

int initilaize_socket(int port) {
  int sd;

  // Create server socket
  if((sd = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
    printf("socket error");
    return -1;
  }

  int option_value = 0;
  setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, (char*) &option_value, sizeof(option_value));

  if (make_socket_nonblocking(sd) == -1) {
    printf("failed to set socket to nonblocking mode");
    return -1;
  }

  struct sockaddr_in addr;
  bzero(&addr, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = INADDR_ANY;

  // Bind socket to address
  if (bind(sd, (struct sockaddr*) &addr, sizeof(addr)) != 0) {
    printf("bind error");
  }

  // Start listing on the socket
  if (listen(sd, LISTEN_QUEUE_LENGTH) < 0) {
    printf("listen error");
    return -1;
  }

  return sd;  
}

int main() {
  int sd = initilaize_socket(PORT_NO);
  if (sd == -1) {
    printf("failed to initialize socket");
    return -1;
  }

  pid_t pid = create_child_process(sd);
  if (pid > 0) {
    // parent (this) process
    // watch child processes
    struct ev_loop *loop = EV_DEFAULT;
    watch_child(loop, sd, pid);

    while (1) {
      ev_loop(loop, 0);
    }
  } else {
    // failure to create child process
    printf("failed to create child process: %d\n", pid);
  }

  return 0;
}
