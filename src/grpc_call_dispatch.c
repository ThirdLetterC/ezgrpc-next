/* grpc_call_dispatch.c - delivering ready calls to their routes (handlers, stream handlers, events)
 *
 * Part of the call layer described in grpc_call.c.
 */

#include <stdarg.h>
#include <stdio.h>
#include "internal/grpc_platform.h"

#include "core.h"
#include "internal_call.h"
#include "internal_helpers.h"
#include "grpc_timeout.h"

/* ---- dispatch ------------------------------------------------------------------ */

/* Appends an event for grpc_server_events(), taking a reference on the call and ownership of
 * `message`. If the array cannot grow, logs and drops the event (freeing the message). */
static void push_event(grpc_server *server, grpc_event_type type, grpc_call *call,
                       grpc_message *message) {
  if (server->nb_events == server->cap_events) {
    size_t cap = server->cap_events ? server->cap_events * 2 : 64;
    grpc_event *events = grpc_realloc(server->events, cap * sizeof(*events));
    if (events == nullptr) {
      LOG_ERROR(server, "out of memory, dropping an event\n");
      grpc_message_free(message);
      return;
    }
    server->events = events;
    server->cap_events = cap;
  }
  server->events[server->nb_events++] = (grpc_event){
      .type = type,
      .call = grpc_call_ref(call),
      .message = message,
      .userdata = call->route->userdata,
  };
}

/* Releases the events returned by the previous poll: frees their messages and drops their call
 * references. Called at the start of each grpc_server_poll() and on server shutdown. */
void server_clear_events(grpc_server *server) {
  for (size_t i = 0; i < server->nb_events; i++) {
    grpc_message_free((grpc_message *)server->events[i].message);
    grpc_call_unref(server->events[i].call);
  }
  server->nb_events = 0;
}

/* Delivers a unary call once the client half-closed: exactly one request message is required,
 * otherwise the call ends with INTERNAL. The request stays owned by the call (call->request), so
 * the handler may keep using it while it holds a reference. */
static void dispatch_unary(grpc_call *call) {
  if (call->finished || call->closed) {
    call_drop_pending(call);
    return;
  }
  /* wait for the whole request */
  if (call->request_delivered || !call->half_closed)
    return;

  size_t count = msg_queue_count(&call->pending);
  if (count != 1) {
    call_drop_pending(call);
    (void)call_finish_internal(call, GRPC_STATUS_INTERNAL,
                               count == 0 ? "missing request message"
                                          : "more than one request message");
    return;
  }
  call->request = msg_queue_pop(&call->pending);
  call->request_delivered = true;
  if (!interceptors_run_on_recv(call, call->request))
    return;
  call->route->handler(call, call->request, call->route->userdata);
}

/* Delivers the pending messages of a streaming call to on_message (each freed after the
 * callback returns), then on_end once the client half-closed. Stops as soon as the call is
 * finished, discarding the rest. */
static void dispatch_stream(grpc_call *call) {
  const grpc_stream_handler *handler = &call->route->stream;
  grpc_message *msg;
  while (!call->finished && (msg = msg_queue_pop(&call->pending)) != nullptr) {
    if (!interceptors_run_on_recv(call, msg)) {
      grpc_message_free(msg);
      break;
    }
    if (handler->on_message != nullptr)
      handler->on_message(call, msg, call->route->userdata);
    grpc_message_free(msg);
  }
  /* finished calls take no more messages */
  call_drop_pending(call);
  if (call->half_closed && !call->end_delivered && !call->finished) {
    call->end_delivered = true;
    if (handler->on_end != nullptr)
      handler->on_end(call, call->route->userdata);
  }
}

/* Event-route counterpart of dispatch_stream(): turns pending messages, half-close and
 * cancellation into grpc_event entries (the events own the messages) and runs the cancel
 * callback once. */
static void dispatch_events(grpc_call *call) {
  grpc_server *server = call->server;
  grpc_message *msg;
  while (!call->finished && (msg = msg_queue_pop(&call->pending)) != nullptr) {
    if (!interceptors_run_on_recv(call, msg)) {
      grpc_message_free(msg);
      break;
    }
    push_event(server, GRPC_EVENT_MESSAGE, call, msg);
  }
  call_drop_pending(call);
  if (call->half_closed && !call->end_delivered && !call->finished) {
    call->end_delivered = true;
    push_event(server, GRPC_EVENT_END, call, nullptr);
  }
  if (call->cancelled && !call->cancel_delivered) {
    call->cancel_delivered = true;
    push_event(server, GRPC_EVENT_CANCEL, call, nullptr);
    if (call->cancel_fn != nullptr)
      call->cancel_fn(call, call->cancel_userdata);
  }
}

/* Delivers what is new on one call. On first dispatch starts it: installs the stream route's
 * on_cancel, runs the on_call interceptors (a rejection finishes the call and discards all
 * further input) and runs on_start. Calls cancelled before starting are dropped without the
 * application ever seeing them. Finally runs the cancel callback once the application has seen
 * the call (event routes do this in dispatch_events()). */
static void dispatch_call(grpc_call *call) {
  if (!call->started) {
    /* cancelled before the application saw it */
    if (call->cancelled) {
      call_drop_pending(call);
      return;
    }
    call->started = true;
    if (call->route->kind == ROUTE_STREAM && call->route->stream.on_cancel != nullptr) {
      call->cancel_fn = call->route->stream.on_cancel;
      call->cancel_userdata = call->route->userdata;
    }
    if (!interceptors_run_on_call(call)) {
      call->rejected = true;
    } else if (call->route->kind == ROUTE_STREAM && call->route->stream.on_start != nullptr &&
               !call->finished) {
      call->route->stream.on_start(call, call->route->userdata);
    }
  }
  if (call->rejected) {
    call_drop_pending(call);
    return;
  }

  switch (call->route->kind) {
  case ROUTE_UNARY:
    dispatch_unary(call);
    break;
  case ROUTE_STREAM:
    dispatch_stream(call);
    break;
  case ROUTE_EVENTS:
    dispatch_events(call);
    return;
  }

  /* a unary call is cancelled for the application once its handler ran */
  bool seen = call->route->kind != ROUTE_UNARY || call->request_delivered;
  if (call->cancelled && !call->cancel_delivered && seen) {
    call->cancel_delivered = true;
    if (call->cancel_fn != nullptr)
      call->cancel_fn(call, call->cancel_userdata);
  }
}

/* Dispatches every call on the ready queue and drops the queue's reference on each. Server thread
 * only; handlers may queue calls again while this runs, and those are dispatched in the same
 * pass. */
void server_dispatch(grpc_server *server) {
  grpc_call *call;
  while ((call = server->ready_head) != nullptr) {
    server->ready_head = call->ready_next;
    if (server->ready_head == nullptr)
      server->ready_tail = nullptr;
    call->ready_next = nullptr;
    call->queued = false;
    dispatch_call(call);
    grpc_call_unref(call);
  }
}

/* Empties the ready queue without dispatching, dropping its references. Used on shutdown. */
void server_drop_ready(grpc_server *server) {
  grpc_call *call;
  while ((call = server->ready_head) != nullptr) {
    server->ready_head = call->ready_next;
    call->ready_next = nullptr;
    call->queued = false;
    grpc_call_unref(call);
  }
  server->ready_tail = nullptr;
}
