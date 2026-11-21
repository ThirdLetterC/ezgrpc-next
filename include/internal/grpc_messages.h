#pragma once

/* grpc_messages.h - short names for the grpc_message queue
 *
 * grpc_messages is the queue type used for messages received on a stream; the functions below
 * forward to grpc_list_message.h with the same ownership rules.
 */

#include "grpc_list_message.h"

/* grpc_messages aliases grpc_list_message_t. */
typedef grpc_list_message_t grpc_messages;

/* Removes and returns the oldest message (caller owns it), or nullptr if empty. */
static inline grpc_message *grpc_messages_pop(grpc_messages *messages) {
  return grpc_list_message_pop_front(messages);
}

/* Returns the oldest message without removing it, or nullptr if empty. */
static inline grpc_message *grpc_messages_peek_front(grpc_messages *messages) {
  return grpc_list_message_peek_front(messages);
}

/* Appends message (ownership moves to the queue); 0 on success, nonzero if out of memory. */
static inline int grpc_messages_push(grpc_messages *messages, grpc_message *message) {
  return grpc_list_message_push_back(messages, message);
}

/* Creates an empty queue; nullptr on allocation failure. The argument is ignored. */
static inline grpc_messages *grpc_messages_new([[maybe_unused]] void *unused) {
  return grpc_list_message_new(nullptr);
}

/* Returns the number of queued messages. */
static inline size_t grpc_messages_count(grpc_messages *messages) {
  return grpc_list_message_count(messages);
}

/* Frees the remaining messages and the queue. Accepts nullptr. */
static inline void grpc_messages_free(grpc_messages *messages) {
  grpc_list_message_free(messages);
}
