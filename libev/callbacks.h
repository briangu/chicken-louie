#ifndef CS_EV_CALLBACKS

#define CS_EV_CALLBACKS
#include "chicken.h"

#include <ev.h>
#include <stdio.h>

typedef struct cs_ev_timer { 
    ev_timer timer;
    C_word closure;
} cs_ev_timer;

void cs_timer_cb(EV_P_ ev_timer *w, int revents);
ev_timer *cs_new_timer(EV_P_ ev_tstamp delay, ev_tstamp redelay);
void cs_start_timer(EV_P_ ev_timer *w, C_word closure);

#endif
