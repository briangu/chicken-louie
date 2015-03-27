
#include "chicken.h"

#include <ev.h>
#include <stdio.h>

#include "callbacks.h"


void cs_timer_cb(EV_P_ ev_timer *w, int revents){
    struct cs_ev_timer *cet = (struct cs_ev_timer*) w;
    C_callback(cet->closure, 0);
}

ev_timer *cs_new_timer(EV_P_ ev_tstamp delay, ev_tstamp redelay) {
    cs_ev_timer *cet;
    cet = malloc(sizeof(cs_ev_timer));
    ev_timer_init(&cet->timer, cs_timer_cb, delay, redelay);
    return (ev_timer *)cet;
}

void cs_start_timer(EV_P_ ev_timer *w, C_word closure) {
    struct cs_ev_timer *cet = (struct cs_ev_timer*) w;
    cet->closure = closure;
    ev_timer_start(loop, &cet->timer);
}

void cs_stop_timer(EV_P_ ev_timer *w) {
    struct cs_ev_timer *cet = (struct cs_ev_timer*) w;
	ev_timer_stop(loop, &cet->timer);
}

void cs_free_timer(ev_timer *w) {
	free(w);
}

void cs_stop_and_free_timer(EV_P_ ev_timer *w) {
	cs_stop_timer(loop, w);
	cs_free_timer(w);
}

ev_timer *cs_start_new_timer_with_callback(EV_P_ ev_tstamp delay, ev_tstamp redelay, C_word closure) {
    cs_ev_timer *cet = (struct cs_ev_timer*)cs_new_timer(loop, delay, redelay);
    cs_start_timer(loop, (ev_timer *)cet, closure);
    return (ev_timer *)cet;
}
