// libev trampoline module
#include "ev_trampoline.h"

void ev_io_init_trampoline(struct ev_io *watcher, void (*fn)(struct ev_loop_t *loop, struct ev_io *watcher, int revents), fd: ev_fd, events: ev_events) {
	ev_io_init(watch, fn, fd, events);
}
