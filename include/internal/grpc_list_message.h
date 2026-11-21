#pragma once

/* grpc_list_message.h - grpc_list specialised for grpc_message pointers
 *
 * Thin typed wrappers over grpc_list. The list owns the messages it holds only in the sense that
 * grpc_list_message_free() frees the remaining ones; pop functions hand ownership to the caller.
 */

#include "grpc_defs.h"
#include "grpc_list.h"
#include "grpc_message.h"

typedef struct grpc_list_message_t grpc_list_message_t;

/* Creates an empty message list; nullptr on allocation failure. The argument is ignored. */
static inline grpc_list_message_t *grpc_list_message_new([[maybe_unused]] void *unused) {
  return (grpc_list_message_t *)grpc_list_new();
}

/* Frees every queued message with grpc_message_free(), then the list. Accepts nullptr. */
static inline void grpc_list_message_free(grpc_list_message_t *list) {
  if (list == nullptr)
    return;
  grpc_list *l = (grpc_list *)list;
  grpc_message *msg;
  while ((msg = (grpc_message *)grpc_list_pop_front(l)) != nullptr) {
    grpc_message_free(msg);
  }
  grpc_list_free(l);
}

/* Appends message (ownership moves to the list); 0 on success, nonzero if out of memory. */
static inline int grpc_list_message_push_back(grpc_list_message_t *list, grpc_message *message) {
  return grpc_list_push_back((grpc_list *)list, (void *)message);
}

/* Prepends message (ownership moves to the list); 0 on success, nonzero if out of memory. */
static inline int grpc_list_message_push_front(grpc_list_message_t *list, grpc_message *message) {
  return grpc_list_push_front((grpc_list *)list, (void *)message);
}

/* Removes and returns the first message (caller owns it), or nullptr if empty. */
static inline grpc_message *grpc_list_message_pop_front(grpc_list_message_t *list) {
  return (grpc_message *)grpc_list_pop_front((grpc_list *)list);
}

/* Removes and returns the last message (caller owns it), or nullptr if empty. */
static inline grpc_message *grpc_list_message_pop_back(grpc_list_message_t *list) {
  return (grpc_message *)grpc_list_pop_back((grpc_list *)list);
}

/* Returns the first message without removing it, or nullptr if empty. */
static inline grpc_message *grpc_list_message_peek_front(grpc_list_message_t *list) {
  return (grpc_message *)grpc_list_peek_front((grpc_list *)list);
}

/* Returns the last message without removing it, or nullptr if empty. */
static inline grpc_message *grpc_list_message_peek_back(grpc_list_message_t *list) {
  return (grpc_message *)grpc_list_peek_back((grpc_list *)list);
}

/* Returns the number of queued messages. */
static inline size_t grpc_list_message_count(grpc_list_message_t *list) {
  return grpc_list_count((grpc_list *)list);
}

/* Returns nonzero if no message is queued. */
static inline int grpc_list_message_is_empty(grpc_list_message_t *list) {
  return grpc_list_is_empty((grpc_list *)list);
}

/* Moves every message of src to the back of dst, leaving src empty. */
static inline void grpc_list_message_concat_and_empty_src(grpc_list_message_t *dst,
                                                          grpc_list_message_t *src) {
  grpc_list_concat_and_empty_src((grpc_list *)dst, (grpc_list *)src);
}
