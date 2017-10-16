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
#include <stdbool.h>
#include <stdint.h>
#include "tuple_compare.h"
#include "memtx_tree.h"

/* {{{ Memtx tree of tuples class. ******************************/

/** Struct that is used as a key in BPS tree definition. */
struct memtx_tuple_tree_key_data {
	/** Sequence of msgpacked search fields. */
	const char *key;
	/** Number of msgpacked search fields. */
	uint32_t part_count;
};

#define MEMTX_TREE_NAME memtx_tuple_tree
#define memtx_tree_elem struct tuple *
#define memtx_tree_key struct memtx_tuple_tree_key_data
#define MEMTX_TREE_ELEM_CMP(elem_a_ptr, elem_b_ptr, key_def)			\
	tuple_compare(*elem_a_ptr, *elem_b_ptr, key_def)
#define MEMTX_TREE_ELEM_WITH_KEY_CMP(elem_ptr, key_ptr, key_def)		\
	tuple_compare_with_key(*elem_ptr, (key_ptr)->key,			\
			       (key_ptr)->part_count, key_def)
#define MEMTX_TREE_ELEM_SET(elem_ptr, tuple, key_def)				\
	({(void)key_def; *elem_ptr = tuple;})
#define MEMTX_TREE_KEY_SET(key_ptr, key_val, part_count_val, key_def)		\
	({(void)key_def; (key_ptr)->key = key_val;				\
	 (key_ptr)->part_count = part_count_val;})
#define MEMTX_TREE_ELEM_GET(elem_ptr) (*(elem_ptr))
#define MEMTX_TREE_KEY_GET(key_ptr, part_count_ptr)				\
	({*part_count_ptr = (key_ptr)->part_count; (key_ptr)->key;})

#include "memtx_tree_impl.h"

#undef memtx_tree_key
#undef memtx_tree_elem
#undef MEMTX_TREE_KEY_GET
#undef MEMTX_TREE_ELEM_GET
#undef MEMTX_TREE_KEY_SET
#undef MEMTX_TREE_ELEM_SET
#undef MEMTX_TREE_ELEM_WITH_KEY_CMP
#undef MEMTX_TREE_ELEM_CMP
#undef MEMTX_TREE_ELEM_EQUAL
#undef memtx_tree_key
#undef memtx_tree_elem
#undef MEMTX_TREE_NAME

/* }}} */

/* {{{ Memtx hinted and hint_only tree class. *******************/

/**
 * Struct that is used as a key in BPS tree definition in
 * memtx_hint_only_tree and memtx_hinted_tree.
*/
struct memtx_hinted_tree_key_data {
	/** Sequence of msgpacked search fields. */
	const char *key;
	/** Number of msgpacked search fields. */
	uint32_t part_count;
	/**
	 * Compare hint. Is calculated automatically on 'set'
	 * operation with memtx_hinted_tree_key_data_set().
	 */
	uint64_t hint;
};

/**
 * Struct that is used as a key in BPS tree definition in
 * memtx_hint_only_tree and memtx_hinted_tree.
 */
struct memtx_hinted_tree_data {
	/** Tuple this node is representing. */
	struct tuple *tuple;
	/**
	 * Compare hint. Is calculated automatically on 'set'
	 * operation with memtx_hinted_tree_data_set().
	 */
	uint64_t hint;
};

/**
 * Compare memtx_hinted_tree records.
 */
static int
memtx_hinted_tree_data_cmp(struct memtx_hinted_tree_data *a,
			   struct memtx_hinted_tree_data *b,
			   struct key_def *key_def)
{
	if (a->hint != b->hint)
		return a->hint < b->hint ? -1 : 1;
	return tuple_compare(a->tuple, b->tuple, key_def);
}

/**
 * Compare memtx_hinted_tree record with key.
 */
static int
memtx_hinted_tree_data_cmp_with_key(struct memtx_hinted_tree_data *a,
				    struct memtx_hinted_tree_key_data *key,
				    struct key_def *key_def)
{
	if (a->hint != key->hint)
		return a->hint < key->hint ? -1 : 1;
	return tuple_compare_with_key(a->tuple, key->key, key->part_count,
				      key_def);
}

/**
 * Initialize memtx_hinted_tree or memtx_hint_only_tree record
 * with tuple and recalculate internal hint field.
 */
static void
memtx_hinted_tree_data_set(struct memtx_hinted_tree_data *data,
			   struct tuple *tuple, struct key_def *key_def)
{
	data->tuple = tuple;
	data->hint = tuple != NULL ? tuple_hint(tuple, key_def) : 0;
}

/**
 * Initialize memtx_hinted_tree or memtx_hint_only_tree key with
 * key raw and part count and recalculate internal hint field.
 */
static void
memtx_hinted_tree_key_data_set(struct memtx_hinted_tree_key_data *key_data,
			       const char *key, uint32_t part_count,
			       struct key_def *key_def)
{
	key_data->key = key;
	key_data->part_count = part_count;
	key_data->hint = key != NULL && part_count > 0 ?
			 key_hint(key, key_def) : 0;
}

#define MEMTX_TREE_NAME memtx_hinted_tree
#define memtx_tree_elem struct memtx_hinted_tree_data
#define memtx_tree_key struct memtx_hinted_tree_key_data
#define MEMTX_TREE_ELEM_EQUAL(elem_a_ptr, elem_b_ptr)				\
	((elem_a_ptr)->tuple == (elem_b_ptr)->tuple)
#define MEMTX_TREE_ELEM_CMP(elem_a_ptr, elem_b_ptr, key_def)			\
	memtx_hinted_tree_data_cmp(elem_a_ptr, elem_b_ptr, key_def)
#define MEMTX_TREE_ELEM_WITH_KEY_CMP(elem_ptr, key_ptr, key_def)		\
	memtx_hinted_tree_data_cmp_with_key(elem_ptr, key_ptr, key_def)
#define MEMTX_TREE_ELEM_SET(elem_ptr, tuple, key_def)				\
	memtx_hinted_tree_data_set(elem_ptr, tuple, key_def)
#define MEMTX_TREE_KEY_SET(key_ptr, key_val, part_count_val, key_def)		\
	memtx_hinted_tree_key_data_set(key_ptr, key_val, part_count_val,	\
				       key_def)
#define MEMTX_TREE_ELEM_GET(elem_ptr) ((elem_ptr)->tuple)
#define MEMTX_TREE_KEY_GET(key_ptr, part_count_ptr)				\
	({*part_count_ptr = (key_ptr)->part_count; (key_ptr)->key;})

#include "memtx_tree_impl.h"

#undef memtx_tree_key
#undef memtx_tree_elem
#undef MEMTX_TREE_KEY_GET
#undef MEMTX_TREE_ELEM_GET
#undef MEMTX_TREE_KEY_SET
#undef MEMTX_TREE_ELEM_SET
#undef MEMTX_TREE_ELEM_WITH_KEY_CMP
#undef MEMTX_TREE_ELEM_CMP
#undef MEMTX_TREE_ELEM_EQUAL
#undef memtx_tree_key
#undef memtx_tree_elem
#undef MEMTX_TREE_NAME

/* }}} */

struct index *
memtx_tree_index_new(struct memtx_engine *memtx, struct index_def *def)
{
	if (def->cmp_def->parts->type == FIELD_TYPE_STRING)
		return memtx_hinted_tree_index_new(memtx, def);
	return memtx_tuple_tree_index_new(memtx, def);
}
