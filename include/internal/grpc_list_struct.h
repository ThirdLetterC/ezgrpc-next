#pragma once

/* grpc_list_struct.h - grpc_list layout
 *
 * Exposes the node and list structs so a grpc_list can be embedded by value (e.g. in a stream or
 * call) and initialised with grpc_list_init() instead of allocated with grpc_list_new().
 */
#include <stdlib.h>
#include "grpc_list.h"

/* One list node, allocated by the push functions and freed when its value is removed. */
typedef struct eznode_t eznode_t;
struct eznode_t {
  eznode_t *next;
  eznode_t *prev;
  void *data;
};

struct grpc_list {
  /* Think of the list as front -> back. The front has no previous node, and
   * the back has no next node. */
  eznode_t *front;
  eznode_t *back;
  size_t count;
};

/* Initialises an embedded list as empty. Does not free any existing nodes; an embedded list is
 * emptied by popping its values, never with grpc_list_free(). */
static inline void grpc_list_init(grpc_list *list) {
  list->front = nullptr;
  list->back = nullptr;
  list->count = 0;
}
