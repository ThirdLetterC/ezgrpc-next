/* grpc_list.c - non-intrusive doubly linked list of void pointers
 *
 * A generic front/back list used internally (e.g. the client's queue of received messages). Each
 * element lives in a separately allocated node (grpc_malloc); the list never owns or frees the
 * stored pointers, only its nodes. A grpc_list can be heap-allocated with grpc_list_new or
 * embedded in another struct and set up with grpc_list_init (grpc_list_struct.h); embedded lists
 * must be drained rather than passed to grpc_list_free. Not thread-safe.
 */

#include <stdlib.h>
#include <assert.h>
#include "core.h"
#include "grpc_list.h"
#include "grpc_list_struct.h"

/* Allocates an empty list; returns nullptr on allocation failure. Free with grpc_list_free. */
grpc_list *grpc_list_new(void) {
  grpc_list *ezlist = grpc_malloc(sizeof(*ezlist));
  if (ezlist == nullptr)
    return nullptr;
  ezlist->front = nullptr;
  ezlist->back = nullptr;
  ezlist->count = 0;
  return ezlist;
}

/* Frees every node and the list itself (accepts nullptr). The stored values are not freed; the
 * caller must release them first if it owns them. Only for lists from grpc_list_new. */
void grpc_list_free(grpc_list *ezlist) {
  if (ezlist == nullptr)
    return;
  eznode_t *curr = ezlist->front;
  while (curr != nullptr) {
    eznode_t *next = curr->next;
    grpc_free(curr);
    curr = next;
  }
  grpc_free(ezlist);
}

/* Appends data after the back element. Returns 0 on success, 1 on allocation failure. */
int grpc_list_push_back(grpc_list *ezlist, void *data) {
  assert(ezlist != nullptr);
  eznode_t *new_node = grpc_malloc(sizeof(*new_node));
  if (new_node == nullptr)
    return 1;
  new_node->data = data;
  new_node->next = nullptr;
  new_node->prev = nullptr;
  if (ezlist->back != nullptr) {
    ezlist->back->next = new_node;
    new_node->prev = ezlist->back;
    ezlist->back = new_node;
  } else {
    ezlist->front = new_node;
    ezlist->back = new_node;
  }

  ezlist->count++;

  return 0;
}

/* Inserts data before the front element. Returns 0 on success, 1 on allocation failure. */
int grpc_list_push_front(grpc_list *ezlist, void *data) {
  assert(ezlist != nullptr);
  eznode_t *new_node = grpc_malloc(sizeof(*new_node));
  if (new_node == nullptr)
    return 1;
  new_node->data = data;
  new_node->next = ezlist->front;
  new_node->prev = nullptr;
  if (ezlist->front != nullptr)
    ezlist->front->prev = new_node;
  else
    ezlist->back = new_node;
  ezlist->front = new_node;

  ezlist->count++;
  return 0;
}

/* Unlinks the back node, frees it and returns its value; returns nullptr for an empty list (so a
 * stored nullptr value is indistinguishable from empty). */
void *grpc_list_pop_back(grpc_list *ezlist) {
  assert(ezlist != nullptr);
  if (!ezlist->count)
    return nullptr;

  eznode_t *back = ezlist->back;
  void *data = back->data;
  ezlist->back = back->prev;

  if (ezlist->back != nullptr) {
    ezlist->back->next = nullptr;
  } else {
    ezlist->front = nullptr;
    assert(ezlist->count == 1);
  }

  grpc_free(back);
  ezlist->count--;
  return data;
}

/* Unlinks the front node, frees it and returns its value; nullptr if the list is empty. */
void *grpc_list_pop_front(grpc_list *ezlist) {
  assert(ezlist != nullptr);
  if (!ezlist->count)
    return nullptr;

  eznode_t *front = ezlist->front;
  void *data = front->data;
  ezlist->front = front->next;

  if (ezlist->front != nullptr) {
    ezlist->front->prev = nullptr;
  } else {
    ezlist->back = nullptr;
    assert(ezlist->count == 1);
  }

  grpc_free(front);
  ezlist->count--;
  return data;
}

/* Returns the number of elements. */
size_t grpc_list_count(grpc_list *ezlist) {
  return ezlist->count;
}

/* Returns nonzero if the list has no elements. */
int grpc_list_is_empty(grpc_list *ezlist) {
  return !ezlist->count;
}

/* Returns the first value, front to back, for which cmp(value, userdata) returns 0, or nullptr if
 * none matches. The list is not modified. */
void *grpc_list_find(grpc_list *ezlist, int (*cmp)(const void *data, const void *userdata),
                     void *userdata) {
  eznode_t *node = ezlist->front;
  while (node != nullptr && cmp(node->data, userdata)) {
    node = node->next;
  }

  return node != nullptr ? node->data : nullptr;
}

/* Unlinks and frees the first node whose value matches (cmp returns 0) and returns that value, or
 * nullptr if none matches. Only the first match is removed. */
void *grpc_list_remove(grpc_list *ezlist, int (*cmp)(const void *data, const void *userdata),
                       void *userdata) {
  eznode_t *node = ezlist->front;
  void *data = nullptr;
  while (node != nullptr && cmp(node->data, userdata)) {
    node = node->next;
  }
  if (node != nullptr) {
    eznode_t *prev = node->prev;
    eznode_t *next = node->next;
    if (prev != nullptr) {
      prev->next = next;
    } else {
      ezlist->front = next;
    }
    if (next != nullptr) {
      next->prev = prev;
    } else {
      ezlist->back = prev;
    }
    data = node->data;
    grpc_free(node);
    ezlist->count--;
  }

  return data;
}

/* Returns the front value without removing it, or nullptr if the list is empty. */
void *grpc_list_peek_front(grpc_list *ezlist) {
  assert(ezlist != nullptr);
  return ezlist->front != nullptr ? ezlist->front->data : nullptr;
}

/* Returns the back value without removing it, or nullptr if the list is empty. */
void *grpc_list_peek_back(grpc_list *ezlist) {
  assert(ezlist != nullptr);
  return ezlist->back != nullptr ? ezlist->back->data : nullptr;
}

/* Moves all nodes of src to the back of dst in O(1) by relinking (no allocation) and leaves src
 * empty but valid. dst and src must be different lists. */
void grpc_list_concat_and_empty_src(grpc_list *dst, grpc_list *src) {
  if (!src->count)
    return;

  if (dst->front == nullptr) {
    dst->front = src->front;
  }
  if (dst->back != nullptr) {
    dst->back->next = src->front;
    src->front->prev = dst->back;
  } else {
    dst->back = src->back;
  }
  dst->back = src->back;
  dst->count += src->count;

  src->front = nullptr;
  src->back = nullptr;
  src->count = 0;
}

/* Calls func(value, userdata) for each element from front to back. func must not remove elements
 * of this list, since the loop reads the current node's next pointer after the call. */
void grpc_list_foreach(grpc_list *ezlist, void (*func)(const void *data, const void *userdata),
                       void *userdata) {
  for (eznode_t *n = ezlist->front; n != nullptr; n = n->next)
    func(n->data, userdata);
}
