/*
 * Copyright 2010-2019, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "swim_test_ev.h"
#include "trivia/util.h"
#include "swim/swim_ev.h"
#include "tarantool_ev.h"
#include "assoc.h"
#include "small/rlist.h"
#include <stdbool.h>

void
swim_transport_do_loop_step(void);

static double watch = 0;

struct swim_test_watcher {
	struct ev_watcher *base;
	bool is_periodic;
	int revents_mask;
	struct rlist events;
};

struct swim_test_event {
	struct swim_test_watcher *watcher;
	double deadline;
	struct rlist in_queue_events;
	struct rlist in_queue_watcher;
};

static inline int
swim_test_event_cmp(const struct swim_test_event *a,
		    const struct swim_test_event *b)
{
	if (a->deadline < b->deadline)
		return -1;
	if (a->deadline > b->deadline)
		return 1;
	return 0;
}

static RLIST_HEAD(queue_events);

static struct mh_i64ptr_t *watchers = NULL;

static void
swim_test_event_new(struct swim_test_watcher *watcher, double delay)
{
	/*
	 * Create event. Push into the queue, and the watcher's
	 * list.
	 */
	struct swim_test_event *tmp, *e =
		(struct swim_test_event *) malloc(sizeof(*e));
	assert(e != NULL);
	e->watcher = watcher;
	e->deadline = swim_time() + delay;
	rlist_add_tail_entry_sorted(&queue_events, tmp, e, in_queue_events,
				    swim_test_event_cmp);
	rlist_add_tail_entry(&watcher->events, e, in_queue_events);
}

static inline void
swim_test_event_delete(struct swim_test_event *e)
{
	/*
	 * Remove event from the queue, the list.
	 */
	rlist_del_entry(e, in_queue_events);
	rlist_del_entry(e, in_queue_watcher);
	free(e);
}

static inline struct swim_test_watcher *
swim_test_watcher_new(struct ev_watcher *base, bool is_periodic,
		      int revents_mask)
{
	/*
	 * Create watcher, store into the watchers hash.
	 */
	struct swim_test_watcher *w =
		(struct swim_test_watcher *) malloc(sizeof(*w));
	assert(w != NULL);
	w->base = base;
	w->is_periodic = is_periodic;
	w->revents_mask = revents_mask;
	rlist_create(&w->events);
	struct mh_i64ptr_node_t node = {(uint64_t) base, w};
	mh_int_t rc = mh_i64ptr_put(watchers, &node, NULL, NULL);
	(void) rc;
	assert(rc != mh_end(watchers));
	return w;
}

static inline void
swim_test_watcher_delete(struct swim_test_watcher *watcher)
{
	/*
	 * Delete watcher's events. Delete the watcher.
	 */
	struct swim_test_event *e, *tmp;
	rlist_foreach_entry_safe(e, &watcher->events, in_queue_watcher, tmp)
		swim_test_event_delete(e);
	mh_int_t rc = mh_i64ptr_find(watchers, (uint64_t) watcher->base, NULL);
	assert(rc != mh_end(watchers));
	mh_i64ptr_del(watchers, rc, NULL);
	free(watcher);
}

static struct swim_test_watcher *
swim_test_watcher_by_ev(struct ev_watcher *watcher)
{
	mh_int_t rc = mh_i64ptr_find(watchers, (uint64_t) watcher, NULL);
	assert(rc != mh_end(watchers));
	return (struct swim_test_watcher *) mh_i64ptr_node(watchers, rc)->val;
}

double
swim_time(void)
{
	return watch;
}

void
swim_ev_periodic_start(struct ev_loop *loop, struct ev_periodic *base)
{
	/* Create the periodic watcher and one event. */
	struct swim_test_watcher *w =
		swim_test_watcher_new((struct ev_watcher*) base, true,
				      EV_PERIODIC);
	swim_test_event_new(w, base->interval);
}

void
swim_ev_periodic_stop(struct ev_loop *loop, struct ev_periodic *base)
{
	/*
	 * Delete the watcher and its events. Should be only one.
	 */
	struct swim_test_watcher *w =
		swim_test_watcher_by_ev((struct ev_watcher*) base);
	swim_test_watcher_delete(w);
}

void
swim_ev_io_start(struct ev_loop *loop, struct ev_io *io)
{
	/*
	 * Create a watcher. No events.
	 */
	swim_test_watcher_new((struct ev_watcher*) io, false,
			      io->events & (EV_READ | EV_WRITE));
}

void
swim_ev_io_stop(struct ev_loop *loop, struct ev_io *io)
{
	/*
	 * Delete the watcher and its events.
	 */
	struct swim_test_watcher *w =
		swim_test_watcher_by_ev((struct ev_watcher*) io);
	swim_test_watcher_delete(w);
}

static void
swim_do_loop_step(struct ev_loop *loop)
{
	/*
	 * Take next event. Update global watch. Execute its cb.
	 * Do one loop step for the transport.
	 */
	if (! rlist_empty(&queue_events)) {
		struct swim_test_event *e =
			rlist_shift_entry(&queue_events, struct swim_test_event,
					  in_queue_events);
		watch = e->deadline;
		EV_CB_INVOKE(e->watcher->base, e->watcher->revents_mask);
		if (e->watcher->is_periodic) {
			struct ev_periodic *p =
				(struct ev_periodic *) e->watcher->base;
			e->deadline = swim_time() + p->interval;
			rlist_add_tail_entry(&queue_events, e, in_queue_events);
		} else {
			swim_test_event_delete(e);
		}
	}
	swim_transport_do_loop_step();
}

void
swim_test_ev_run_loop(struct ev_loop *loop)
{
	while (true)
		swim_do_loop_step(loop);
}

void
swim_test_ev_init(void)
{
	watchers = mh_i64ptr_new();
	assert(watchers != NULL);
}

void
swim_test_ev_free(void)
{
	mh_int_t rc = mh_first(watchers);
	while (rc != mh_end(watchers)) {
		mh_i64ptr_del(watchers, rc, NULL);
		swim_test_watcher_delete((struct swim_test_watcher *)
					 mh_i64ptr_node(watchers, rc)->val);
	}
	mh_i64ptr_delete(watchers);
	assert(rlist_empty(&queue_events));
}
