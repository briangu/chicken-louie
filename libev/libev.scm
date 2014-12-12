(use extras)

(import foreign)
(foreign-declare "#include <ev.h>")
(foreign-declare "#include <stdio.h>")
(foreign-declare "#include \"callbacks.h\"")

(define-foreign-variable EVFLAG-AUTO "EVFLAG_AUTO")
(define-foreign-variable EVFLAG-NO_ENV "EVFLAG_NO_ENV")
(define-foreign-variable EVFLAG-FORKCHECK "EVFLAG_FORKCHECK")
(define-foreign-variable EVFLAG-NOINOTIFY "EVFLAG_NOINOTIFY")
(define-foreign-variable EVFLAG-SIGNALFD "EVFLAG_SIGNALFD")
(define-foreign-variable EVFLAG-NOSIGMASK "EVFLAG_NOSIGMASK")

(define-foreign-variable EVBACKEND-SELECT "EVBACKEND_SELECT")
(define-foreign-variable EVBACKEND-POLL "EVBACKEND_POLL")
(define-foreign-variable EVBACKEND-EPOLL "EVBACKEND_EPOLL")
(define-foreign-variable EVBACKEND-KQUEUE "EVBACKEND_KQUEUE")
(define-foreign-variable EVBACKEND-DEVPOLL "EVBACKEND_DEVPOLL")
(define-foreign-variable EVBACKEND-PORT "EVBACKEND_PORT")
(define-foreign-variable EVBACKEND-ALL "EVBACKEND_ALL")
(define-foreign-variable EVBACKEND-MASK "EVBACKEND_MASK")

(define-foreign-variable EVBREAK-CANCEL "EVBREAK_CANCEL")
(define-foreign-variable EVBREAK-ONE "EVBREAK_ONE")
(define-foreign-variable EVBREAK-ALL "EVBREAK_ALL")

(define-foreign-variable EV_STDIN "STDIN_FILENO")
(define-foreign-variable EV_STDOUT "STDOUT_FILENO")

(define-foreign-variable EV_READ "EV_READ")
(define-foreign-variable EV_WRITE "EV_WRITE")

(define-foreign-type ev-fd int) ; io fd
(define-foreign-type ev-events int) ; io events
(define-foreign-type ev-tstamp double) ; ev_tstamp
(define-foreign-type ev-loop (c-pointer "struct ev_loop"))

(define-foreign-type ev-io "struct ev_io")
(define-foreign-type *ev-io (c-pointer ev-io))
(define-foreign-type ev-timer "ev_timer")
(define-foreign-type *ev-timer (c-pointer ev-timer))
(define-foreign-type ev-cs-timer "cs_ev_timer")

(define ev-version-major (foreign-lambda int "ev_version_major"))
(define ev-version-minor (foreign-lambda int "ev_version_minor"))
(define ev-supported-backends (foreign-lambda unsigned-int "ev_supported_backends"))
(define ev-recommended-backends (foreign-lambda unsigned-int "ev_recommended_backends"))
(define ev-embeddable-backends (foreign-lambda unsigned-int "ev_embeddable_backends"))
(define ev-time (foreign-lambda ev-tstamp "ev_time"))
(define ev-sleep (foreign-lambda void "ev_sleep" ev-tstamp))
(define ev-feed-signal (foreign-lambda void "ev_feed_signal" int))
(define ev-default-loop (foreign-lambda ev-loop "ev_default_loop" unsigned-int))
(define ev-loop-new (foreign-lambda ev-loop "ev_loop_new" unsigned-int))
(define ev-loop-destroy (foreign-lambda void "ev_loop_destroy" ev-loop))
(define ev-loop-fork (foreign-lambda void "ev_loop_fork" ev-loop))
(define ev-is-default-loop (foreign-lambda bool "ev_is_default_loop" ev-loop))
(define ev-iteration (foreign-lambda unsigned-int "ev_iteration" ev-loop))
(define ev-depth (foreign-lambda unsigned-int "ev_depth" ev-loop))
(define ev-backend (foreign-lambda unsigned-int "ev_backend" ev-loop))
(define ev-now (foreign-lambda ev-tstamp "ev_now" ev-loop))
(define ev-now-update (foreign-lambda void "ev_now_update" ev-loop))
(define ev-suspend (foreign-lambda void "ev_suspend" ev-loop))
(define ev-resume (foreign-lambda void "ev_resume" ev-loop))
(define ev-run (foreign-safe-lambda void "ev_run" ev-loop int))
(define ev-break (foreign-lambda void "ev_break" ev-loop int))
(define ev-ref (foreign-lambda void "ev_ref" ev-loop))
(define ev-unref (foreign-lambda void "ev_unref" ev-loop))
(define ev-unloop (foreign-lambda void "ev_unloop" ev-loop int))

(define ev-timer-init (foreign-lambda void "ev_timer_init" *ev-timer (function void (ev-loop *ev-timer int)) ev-tstamp ev-tstamp))
(define ev-timer-start (foreign-lambda void "ev_timer_start" ev-loop *ev-timer))
(define ev-timer-stop (foreign-lambda void "ev_timer_stop" ev-loop *ev-timer))

(define cs-new-timer (foreign-lambda *ev-timer "cs_new_timer" ev-loop ev-tstamp ev-tstamp))
(define cs-start-timer (foreign-lambda void"cs_start_timer" ev-loop *ev-timer scheme-object))
(define cs-start-new-timer (foreign-lambda void"cs_start_new_timer_with_callback" ev-loop ev-tstamp ev-tstamp scheme-object))
(define cs-stop-timer (foreign-lambda void"cs_stop_timer" ev-loop *ev-timer))
(define cs-stop-and-free-timer (foreign-lambda void"cs_stop_and_free_timer" ev-loop *ev-timer))
(define cs-free-timer (foreign-lambda void"cs_free_timer" *ev-timer))

(define ev-io-init (foreign-lambda void "ev_io_init" *ev-io (function void (ev-loop *ev-io int)) ev-fd ev-events))
(define ev-io-start (foreign-lambda void "ev_io_start" ev-loop *ev-io))

; main

; create the event loop
(define l (ev-default-loop 0))

(display "Default: ")(display (ev-is-default-loop l))(newline)
(display "Iteration: ")(display (ev-iteration l))(newline)
(display "Depth: ")(display (ev-depth l))(newline)
(display "Backend: ")(display (ev-backend l))(newline)

; show how to use an externally allocated timer 
(define mk-ev-timer 
	(foreign-lambda* *ev-timer ()
		"C_return(malloc(sizeof(ev_timer)));"))

(define timeout_watcher (mk-ev-timer))
(define k 0)

(define-external (hello_cb (ev-loop l) 
		 	  		 (*ev-timer timer)
		 	  		 (int n)) 
	void 
	(print "Hello, world! what's up?")
   	(set! k (+ k 1))
   	(print k)
   	(if (> k 3)
   		(begin 
   			(print "stopping timer")
   		 	(ev-timer-stop l timeout_watcher))))

(ev-timer-init timeout_watcher #$hello_cb 0 1)
(ev-timer-start l timeout_watcher)

; show how to create a simple timer

(let ((z 60))
	(cs-start-new-timer l 0 1.0
		(lambda ()
			(display z)(newline)
			(set! z (+ z 1)))))

; show how to create a timer that calls a lambda

(let* ((z 32)
	   (zt (cs-new-timer l 0 1.0)) ; allocate the timer
	   (zl (lambda () ; create a lambda which has the timer in the closure
				(display z)(newline)
				(set! z (+ z 1))
				(if (> z 40)
					(begin 
						(print "stopping timer: zt")
				 		(cs-stop-and-free-timer l zt))))))
	(cs-start-timer l zt zl)) ; start the timer

; io

(define mk-ev-io 
	(foreign-lambda* *ev-io ()
		"C_return(malloc(sizeof(ev_io)));"))
(define stdin_watcher (mk-ev-io))

(define-external (stdin_cb (ev-loop l) 
		 	  		 (*ev-io fd)
		 	  		 (int n)) 
	void 
	(display "got some data: ")
	(print (read-line)))

(ev-io-init stdin_watcher #$stdin_cb 0 1)
(ev-io-start l stdin_watcher)


; start the loop

(ev-run l 0)
(display "Done")(newline)
(ev-loop-destroy l)
(display "Destroyed")(newline)
