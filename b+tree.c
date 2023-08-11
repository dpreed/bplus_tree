/*
 * Simple implementation of B+ Tree in C.
 * Copyright (c) 2020-2021 David P. Reed
 *  
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute, 
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
//#define CHECK_INVARIANTS
#ifdef CHECK_INVARIANTS
#include <stdio.h>
#endif
#include <stdio.h>

#define USE_MMAP_ANON
#ifdef USE_MMAP_ANON
#include <sys/mman.h>
#endif

#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include "b+tree.h"

/* blocks are the size of one page, or 512 64-bit words in memory. */

struct header {
	unsigned char num_keys;	/* at most 255 keys are stored in a leaf or index node */
};

union word {
	lkey_t key;
	value_t value;
	struct block *child;
	struct block *leaf;
	struct header header;
};

static const unsigned WORDSIZE = sizeof(union word);

struct block {
	union word words[512];
};

/* copy words of a block, cannot overlap */
static inline void wrdcpy(union word *d, union word *s, unsigned nw)
{
	memcpy(d, s, nw * WORDSIZE);
}

/* copy words of a block, handling overlap to open space in array */
static inline void wrdmove(union word *d, union word *s, unsigned nw)
{
	memmove(d, s, nw * WORDSIZE);
}

typedef struct block * blkp;

/* layout of block, with max 255 keys, 256 children or 255 values, min keys = 128 */
static const int ORDER = 256; /* max children of node (max keys is one less) */
static const unsigned LHALF = ORDER/2;/* after split, left half of node children plus 1 stay put */
static const unsigned RHALF = ORDER/2; /* after split, right half of node has this many children  */

enum {
	HEADER = 0,
	KEY_0 = 1,		/* in a leaf there are ORDER-1 keys */
	FIELD_0 = 256,	/* in a leaf there are ORDER-1 values , in an index node ORDER children  */
	NEXT = 2 * 256 - 1 	/* NEXT exists only in a leaf, where there are at most ORDER-1 values */
};

/* block memory management */

/* block nodes are page aligned. PAGESIZE is the size of a page of memory, for alignment of blocks */
#define PAGE_BITS (12)
#define PAGESIZE (1UL << PAGE_BITS)
#define PAGE_ALIGNED_SIZE(n) ((n + (PAGESIZE - 1)) & ~(PAGESIZE - 1))

static inline blkp alloc_page_for_block(void)
{
#ifdef USE_MMAP_ANON
	return (blkp) mmap(NULL, PAGESIZE, PROT_READ|PROT_WRITE,
			   MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE,
			   -1, 0);
#else
	return (blkp) aligned_alloc(PAGESIZE, PAGE_ALIGNED_SIZE(sizeof(struct block)));
#endif
}

static inline void free_page_for_block(blkp b)
{
#ifdef USE_MMAP_ANON
	munmap(b, PAGESIZE);
#else
	free(b);
#endif
}

static inline blkp new_index_block(void)
{
	return alloc_page_for_block();
}

static inline blkp new_leaf_block(void)
{
	return alloc_page_for_block();
}

static inline void free_index_block(blkp b)
{
	free_page_for_block(b);
}

static inline void free_leaf_block(blkp b)
{
	free_page_for_block(b);
}


/* block accessor functions */

static inline unsigned num_keys(blkp b)
{
	return b->words[HEADER].header.num_keys;
}

static inline lkey_t get_key(blkp b, unsigned i)
{
	return b->words[KEY_0 + i].key;
}

/* return index of first key in leaf that is >= k, or if no such key, the number of keys currently in the leaf */
static unsigned scan_leaf_keys(blkp b, lkey_t k)
{
	unsigned i;
	unsigned nk = num_keys(b);
	/* perhaps use binary search to be a fair bit faster? */
	for (i = 0; i < nk && k > get_key(b, i); i++);
	return i;
}

/* return index of first key in node that is > k, or if no such key, the number of keys currently in the node */
static unsigned scan_index_keys(blkp b, lkey_t k)
{
	unsigned i;
	unsigned nk = num_keys(b);
	/* perhaps use binary search to be a fair bit faster? */
	for (i = 0; i < nk && k >= get_key(b, i); i++);
	return i;
}

/* index nodes have children */
static blkp get_child(blkp b, unsigned i)
{
	return b->words[FIELD_0 + i].child;
}

/* leaf nodes have values where child pointers would be */
static value_t get_value(blkp b, unsigned i)
{
	return b->words[FIELD_0 + i].value;
}

static void set_value(blkp b, unsigned i, value_t v)
{
	b->words[FIELD_0 + i].value = v;
}

static blkp next_leaf(blkp b)
{
	return b->words[NEXT].leaf;
}

static void set_next_leaf(blkp leaf, blkp next)
{
	leaf->words[NEXT].leaf = next;
}

/* during index traversal an array of path node info is accumulated to support splitting */
struct path_node {
	 blkp node;/* node in path */
	 unsigned num_keys;/* number of keys in node */
	 unsigned pos;/* index of path child to split node after */
	 blkp split;/* new node allocated to split full node into */
 };
 
/* b+ tree object */
struct bplus {
	blkp root;/* current root of index tree */
	blkp leaves;/* list of leaf nodes for sequential traversal */
	unsigned depth; /* current b tree depth */
	bplus_cursor_t cursor_list;
	unsigned long num_recs;
	unsigned long num_blks;
	unsigned long num_crsrs;
	unsigned path_length;/* length of allocated path array, must be >= depth */
	struct path_node *path;/* holds path traversed to current index depth */
	blkp new_root;/* to hold a new root block for splitting root */
};

struct bplus_cursor {
	bplus_t tree;/* if active, points to its tree, else NULL */
	bplus_cursor_t next;/* list thread of active cursors for a tree */
	blkp leaf;/* leaf containing current record */
	unsigned pos;/* index of current record in leaf */
	unsigned invalid;/* set if record at cursor was deleted, but next_cursor will still work */
};

/* make a cursor for tree and thread into tree's list of cursors */
static bplus_cursor_t make_bplus_cursor(bplus_t b, blkp leaf, unsigned pos)
{
	bplus_cursor_t c = malloc(sizeof(struct bplus_cursor));
	if (c != NULL) {
		b->num_crsrs += 1;
		c->tree = b;
		c->next = b->cursor_list;
		b->cursor_list = c;
		c->leaf = leaf;
		c->pos = pos;
		c->invalid = 0;
	}
	return c;
}


/* make a new bplus tree */
bplus_t new_bplus_tree(void)
{
	bplus_t b = malloc(sizeof(struct bplus));
	if (b != NULL) {
		/* create initial root as an empty leaf */
		b->root = new_leaf_block();
		if (b->root == NULL) {
			free(b);
			return NULL;
		}
		b->leaves = b->root;
		b->root->words[HEADER].header.num_keys = 0;
		set_next_leaf(b->root, NULL);
		b->num_blks = 1;

		b->num_recs = b->num_crsrs = 0;

		b->path = NULL;
		b->depth = 0;
		b->path_length = 0;
		b->new_root = NULL;
		b->cursor_list = NULL;
	}
	return b;
}

/* reserve a path record sufficient for current depth of index */
static enum bplus_error path_reserved(bplus_t b)
{
	if (b->path_length < b->depth) {
		if (b->path != NULL)
			free(b->path);
		b->path_length = b->depth;
		b->path = malloc(b->path_length * sizeof(struct path_node));
		if (b->path == NULL)
			return NOMEM;
	}
	return OK;
}

static void free_index_subtree(bplus_t b, unsigned d, blkp blk)
{
	if (d < b->depth) {
		for (unsigned i = 0; i <= num_keys(blk); i++)
			free_index_subtree(b, d + 1, blk->words[FIELD_0 + i].child);
		free_index_block(blk);
		b->num_blks -= 1;
	} else {
		free_leaf_block(blk);
		b->num_blks -= 1;
	}
}

void free_bplus_tree(bplus_t b)
{
	/* deactivate all cursors of tree */
	for (bplus_cursor_t bc = b->cursor_list; bc != NULL; bc = bc->next)
		bc->tree = NULL;
	free_index_subtree(b, 0, b->root);
	free(b->path);
}

/* undo preallocations if incomplete */
static void free_preallocated_splits(bplus_t b, unsigned d)
{
	if (d == 0 && b->new_root != NULL)
		free_index_block(b->new_root);
	for (; d < b->depth; d++)
		free_index_block(b->path[d].split);
}

/* To avoid need to allocate (which can fail) do all allocation for splitting leaf and index nodes */
static blkp preallocate_splits(bplus_t b)
{
	blkp split_leaf = NULL;
	unsigned d;
	unsigned n_allocs = 0;
	b->new_root = NULL;
	/* preallocate all index nodes that will need to be used in split */
	for (d = b->depth; d != 0 && b->path[--d].num_keys == ORDER - 1;) {
		b->path[d].split = new_index_block();
		if (b->path[d].split == NULL) {
			free_preallocated_splits(b, d + 1);
			return split_leaf;
		}
		n_allocs += 1;
	}
	/* if either no index or reacjed top node which is full */
	if (b->depth == 0 || (d == 0 && b->path[d].num_keys == ORDER - 1)) {
		if (b->depth != 0) {
			b->path[d].split = new_index_block();
			if (b->path[d].split == NULL) {
				free_preallocated_splits(b, d + 1);
				return split_leaf;
			}
			n_allocs += 1;
		}
		b->new_root = new_index_block();
		if (b->new_root == NULL) {
			free_preallocated_splits(b, d);
			return split_leaf;
		}
		n_allocs += 1;
	}
	split_leaf = new_leaf_block();
	if (split_leaf == NULL)
		free_preallocated_splits(b, d);
	else b->num_blks += n_allocs + 1;
	return split_leaf;
}

/* ***** B+ Tree operations ***** */

void get_active_storage(bplus_t b, unsigned long *num_records, unsigned long *num_blocks, unsigned long *num_cursors)
{
	*num_records = b->num_recs;
	*num_blocks = b->num_blks;
	*num_cursors = b->num_crsrs;
}

 
/* find leaf which should contain key and position that should contain key */
static blkp find_leaf(bplus_t b, lkey_t k)
{
	blkp node = b->root;
	/* scan through index nodes above leaves, recording path */
	for (unsigned d = 0; d < b->depth; d++) {
		unsigned i = scan_index_keys(node, k); /* i is index of first key larger than or equal to k */
		
		b->path[d].node = node;
		b->path[d].pos = i;/* where a new split child key would be inserted */
		b->path[d].num_keys = num_keys(node);
		node = get_child(node, i); /* the i'th child is the child containing keys < k */
	}
	return node;
}

/* find leaf node and value corresponding to key or fail with not found */
enum bplus_error find(bplus_t b, lkey_t k, value_t *v)
{
	if (b->root != NULL) {
		enum bplus_error ok = path_reserved(b);
		if (ok != OK) return ok;
		{
			blkp leaf = find_leaf(b, k);
			/* scan leaf keys for match */
			unsigned i = scan_leaf_keys(leaf, k);
			/* i is the first key >= k */
			if (k == get_key(leaf, i)) {
				*v = get_value(leaf, i);
				return OK;
			}
			/* key isn't in leaf, if key at i is != k */
		}
	}
	return NOTFOUND;
}

/* insert key and value into leaf at insertion point i, moving remaining keys */
static inline blkp insert_into_leaf(bplus_t b, blkp leaf, unsigned i, lkey_t key, value_t v)
{
	unsigned nk = num_keys(leaf);
	if (nk - i != 0) {
		wrdmove(leaf->words + KEY_0 + i + 1, leaf->words + KEY_0 + i, nk - i);
		wrdmove(leaf->words + FIELD_0 + i + 1, leaf->words + FIELD_0 + i, nk - i);
	}
	leaf->words[KEY_0 + i].key = key;
	leaf->words[FIELD_0 + i].value = v;
	leaf->words[HEADER].header.num_keys = nk + 1;
	/* adjust cursors pointing after this */
	for (bplus_cursor_t bc = b->cursor_list; bc != NULL; bc = bc->next)
		if (bc->leaf == leaf && bc->pos >= i) bc->pos += 1;
	return leaf;
}

/* insert splitting key and child node into node AFTER split child's key at i - 1, moving remaining keys */
static blkp insert_split_into_index(blkp node, unsigned i, lkey_t key, blkp child)
{
	unsigned nk = num_keys(node);
	if (nk - i != 0) {
		wrdmove(node->words + KEY_0 + i + 1, node->words + KEY_0 + i, nk - i);
		wrdmove(node->words + FIELD_0 + i + 2, node->words + FIELD_0 + i + 1, nk - i);
	}
	node->words[KEY_0 + i].key = key;
	node->words[FIELD_0 + i + 1].child = child;
	node->words[HEADER].header.num_keys = nk + 1;
	return node;
}

static blkp split_index(bplus_t b, blkp parent, blkp newp, unsigned pos, lkey_t *k, blkp new)
{
	/* 
	 * Splits index node parent adding peer newp, Initially,
	 * parent has ORDER - 1 keys, ORDER children, adding one key *k and one child new 
	 * with new key at position pos and new child at pos+1 in overall sequence.
	 * On return *k will contains new splitting key for new index block newp.
	 *
	 * parent and newp will then have LHALF + 1 and RHALF children, LHALF and RHALF-1 keys.
	 * The new splitting key will be the key numbered LHALF in the combined sequence of ORDER
	 * keys, and will be left in parent at key[LHALF].
	 */
	/* First setup new node sizes: */
	parent->words[HEADER].header.num_keys = LHALF;
	newp->words[HEADER].header.num_keys = RHALF - 1;
#define CAREFUL // carefully coded version of split and insert
#ifdef CAREFUL
	/* careful copy of children to right node, with insert at pos */
	unsigned j = RHALF - 1; /* j is target position (starting in new child) */
	blkp dnode = newp;
	if (pos == ORDER - 1) {
		dnode->words[FIELD_0 + RHALF - 1].child = new;
		j -= 1;
	}
	for (unsigned i = ORDER; i-- > 0;) {/* i is the source position in parent */
		if (i < pos && i < LHALF + 1) break; /* insertion and split are complete */
		dnode->words[FIELD_0 + j] = parent->words[FIELD_0 + i]; 
		if (j-- == 0) {
			dnode = parent;
			j = LHALF;
		}
		if (i == pos + 1) { /* inserting new child before this child */
			dnode->words[FIELD_0 + j].child = new;
			if (j-- == 0) {
				dnode = parent;
				j = LHALF;
			}
		}
	}
	/* careful copy of keys to right node, with insert at pos */
	j = RHALF - 2; /* j is the target position (starting in new child) */
	dnode = newp;
	if (pos == ORDER - 1) {
		dnode->words[KEY_0 + RHALF - 2].key = *k;
		j -= 1;
	}
	for (unsigned i = ORDER - 1; i-- > 0; ) {/* i is the source position in parent */
		if (i < pos && i < LHALF) break; /* insertions and split are complete */
		dnode->words[KEY_0 + j] = parent->words[KEY_0 + i];
		if (j-- == 0) {
			dnode = parent;
			j = LHALF;
		}
		if (i == pos) {/* inserting new key before this key */
			dnode->words[KEY_0 + j].key = *k;
			if (j-- == 0) {
				dnode = parent;
				j = LHALF;
			}
		}
	}
#else // optimized version of careful code using fast memory copy/move
	/* Now move the keys and children into the new node, and insert new key and child */
	if (pos < LHALF) { /* inserting new child into left part of split node */
		/* copy rightmost RHALF-1  keys to newp, and RHALF children to newp */
		wrdcpy(newp->words + KEY_0, parent->words + KEY_0 + LHALF, RHALF - 1);
		wrdcpy(newp->words + FIELD_0, parent->words + FIELD_0 + LHALF, RHALF);
		/* Now there are ORDER -1 - (RHALF - 1) == LHALF keys in parent, and ORDER - RHALF == LHALF children  */
		/* then insert *k into parent keys at pos i, and new after at field pos i + 1 */
		/* open up space for new key and child, moving at least one key to the right */
		wrdmove(parent->words + KEY_0 + pos + 1, parent->words + KEY_0 + pos, LHALF - pos);
		if (LHALF - pos - 1 != 0)
			wrdmove(parent->words + FIELD_0 + pos + 2, parent->words + FIELD_0 + pos + 1, LHALF - pos - 1);
		/* insert new key and child */
		parent->words[KEY_0 + pos].key = *k;
		parent->words[FIELD_0 + pos + 1].child = new;
	} else if (pos == LHALF) {
		/* inserting new child just at right of split, promoting *k again, and putting new first in right node */
		wrdcpy(newp->words + KEY_0, parent->words + KEY_0 + LHALF, RHALF - 1);
		wrdcpy(newp->words + FIELD_0 + 1, parent->words + FIELD_0 + LHALF + 1, RHALF - 1);
		parent->words[KEY_0 + LHALF] = *k;
	} else /* LHALF < i < ORDER */ {	/*  both inserted key and new child will be into right part of split node */
		/* copy keys and children prior to new key and child, leaving behind key to be promoted */
		wrdcpy(newp->words + KEY_0, parent->words + KEY_0 + LHALF + 1, pos - (LHALF + 1));
		wrdcpy(newp->words + FIELD_0, parent->words + FIELD_0 + LHALF + 1, pos + 1 - (LHALF + 1));
		/* insert new key and child */
		newp->words[KEY_0 + pos - (LHALF + 1)].key = *k;
		newp->words[FIELD_0 + pos - LHALF].child = new;
		/* copy the rest, after new key */
		if (ORDER - 1 - pos != 0) {
			wrdcpy(newp->words + KEY_0 + pos - LHALF, parent->words + KEY_0 + pos + 2, ORDER - 1 - pos);
			wrdcpy(newp->words + FIELD_0 + pos + 1 - LHALF, parent->words + FIELD_0 + pos + 2, ORDER - 1 - pos);
		}
	}
#endif
	/*
	 * return newly created node and the leftmost key in its subtree, by promoting
	 * the last key to the right left in parent.
	 */
	*k = parent->words[KEY_0 + LHALF].key;
	return newp;

}
 
static blkp split_leaf(bplus_t b, blkp leaf, blkp new, unsigned i, lkey_t *k, value_t v)
{
	/* full leaf: has ORDER-1 keys, ORDER-1 values; two new leaves will have ORDER/2 keys and values */
	/* Note: i < ORDER on entry */
	leaf->words[HEADER].header.num_keys = LHALF;
	new->words[HEADER].header.num_keys = RHALF;
	/* add new linked leaf node via NEXT pointer */
	set_next_leaf(new, next_leaf(leaf));
	set_next_leaf(leaf, new);
	/* insert new key and value into old or new leaf based on where it should have been inserted */
	if (i < LHALF) {/* key will be inserted in left result node */
		wrdcpy(new->words + KEY_0, leaf->words + KEY_0 + LHALF - 1, RHALF);
		wrdcpy(new->words + FIELD_0, leaf->words + FIELD_0 + LHALF - 1, RHALF);
		if (LHALF - 1 - i != 0) {
			wrdmove(leaf->words + KEY_0 + i + 1, leaf->words + KEY_0 + i, LHALF - 1 - i);
			wrdmove(leaf->words + FIELD_0 + i + 1, leaf->words + FIELD_0 + i, LHALF - 1 - i);
		}
		leaf->words[KEY_0 + i].key = *k;
		leaf->words[FIELD_0 + i].value = v;
	} else {/* key will be inserted into right result node */
		if (i > LHALF) {
			wrdcpy(new->words + KEY_0, leaf->words + KEY_0 + LHALF, i - LHALF);
			wrdcpy(new->words + FIELD_0, leaf->words + FIELD_0 + LHALF, i - LHALF);
		}
		new->words[KEY_0 + i - LHALF].key = *k;
		new->words[FIELD_0 + i - LHALF].value = v;
		if (ORDER - 1 - i != 0) {
			wrdcpy(new->words + KEY_0 + i + 1 - LHALF, leaf->words + KEY_0 + i, ORDER - 1 - i);
			wrdcpy(new->words + FIELD_0 + i + 1 - LHALF, leaf->words + FIELD_0 + i, ORDER - 1 - i);
		}
	}
	/* promote leftmost key in new leaf to parent */
	*k = get_key(new, 0);
	for (bplus_cursor_t bc = b->cursor_list; bc != NULL; bc = bc->next) {
		if (bc->leaf == leaf) {
			if (bc->pos >= i) bc->pos += 1;
			if (bc->pos >= LHALF) bc->pos -= LHALF;
		}
	}
	return new;
}

static void add_root_block(bplus_t b, blkp left_child, lkey_t k, blkp right_child)
{
	blkp new = b->new_root;
	new->words[HEADER].header.num_keys = 1;
	new->words[KEY_0].key = k;
	new->words[FIELD_0].child = left_child;
	new->words[FIELD_0 + 1].child = right_child;
	b->root = new;
	b->depth += 1;
}


static void insert_new_leaf(bplus_t b, blkp new, lkey_t *k)
{
		/* insert new into parent chain, splitting nodes as needed */
	for (unsigned d = b->depth; d != 0;) {
		blkp parent = b->path[--d].node;
		unsigned i = b->path[d].pos;
		if (num_keys(parent) < ORDER - 1) {
			/* insert new block into this ancestor, and done */
			insert_split_into_index(parent, i, *k, new);
			return;
		}
		/* full, split this parent, inserting key and getting new block and promoted key */
		new = split_index(b, parent, b->path[d].split, i, k, new);
		/* continue, inserting new and promoted key into its parent */
	}
	/* having split root, must add a node above the current root to hold new and current root */
	add_root_block(b, b->root, *k, new);
}

/* insert new key value pair into B+ tree, returning 0 if insert failed */
enum bplus_error insert(bplus_t b, lkey_t k, value_t v)
{
	/* insure that we don't need to allocate memory during insert */
	enum bplus_error ok = path_reserved(b);
	if (ok == OK) {
		blkp leaf = find_leaf(b, k);
		unsigned nk = num_keys(leaf);
		unsigned i = scan_leaf_keys(leaf, k);
		if (i < nk && get_key(leaf, i) == k)/* key is already present */
			set_value(leaf, i, v);/* update value */
		else if (nk < ORDER-1) {/* has room for new k,v pair */
			insert_into_leaf(b, leaf, i, k, v);
			b->num_recs += 1;
		} else {
			/* must split leaf, preallocate all needed memory */
			blkp split = preallocate_splits(b);
			if (split == NULL) ok = NOMEM;
			else {
				insert_new_leaf(b, split_leaf(b, leaf, split, i, &k, v), &k);
				b->num_recs += 1;
			}
		}
	}
	return ok;
}
/* fix cursors after rotating one item from right peer to leaf */
static void fix_cursor_rotate_left(bplus_t b, blkp leaf, blkp rpeer)
{
	for (bplus_cursor_t bc = b->cursor_list; bc != NULL; bc = bc->next) {
		if (bc->leaf == rpeer) {
			if (bc->pos == 0) {
				bc->leaf = leaf;
				bc->pos = num_keys(leaf) - 1;
			} else {
				bc->pos -= 1;
			}
		}
	}
}

/* fix cursors after rotating one item from left peer to leaf */
static void fix_cursor_rotate_right(bplus_t b, blkp lpeer, blkp leaf)
{
	for (bplus_cursor_t bc = b->cursor_list; bc != NULL; bc = bc->next) {
		if (bc->leaf == leaf) {
			bc->pos += 1;
		} else if (bc->leaf == lpeer && bc->pos == num_keys(lpeer)) {
			bc->leaf = leaf;
			bc->pos = 0;
		}
	}
}

/* merging leaf and rightward peer deletes peer, so correct cursors to peer */
static void fix_cursor_merge(bplus_t b, blkp leaf, blkp peer, unsigned nkl)
{
	for (bplus_cursor_t bc = b->cursor_list; bc != NULL; bc = bc->next) {
		if (bc->leaf == peer) {
			/* leaf had nkl key,value pairs */
			printf("fix cursor leaf %p peer %p nkl %u bc->pos %u",
			       leaf, peer, nkl, bc->pos);
			bc->leaf = leaf;
			bc->pos += nkl;
		}
	}
}

static void merge_index_nodes(bplus_t b, blkp l, blkp r, lkey_t s)
{
	unsigned nkl = num_keys(l);
	unsigned nkr = num_keys(r);
#ifdef CHECK_INVARIANTS
	/* merge overflow should not happen */
	if (nkl + nkr > ORDER - 2) {
		printf("Index node too big to merge, %u %u\n", nkl, nkr);
		exit(EXIT_FAILURE);
	}
#endif
	l->words[KEY_0 + nkl].key = s;
	wrdmove(l->words + KEY_0 + nkl + 1, r->words + KEY_0, nkr);
	wrdmove(l->words + FIELD_0 + nkl + 1, r->words + FIELD_0, nkr + 1);
	l->words[HEADER].header.num_keys += nkr + 1;
#ifdef CHECK_INVARIANTS
	if (l->words[KEY_0 + nkl - 1].key >= l->words[KEY_0 + nkl].key ||
	    l->words[KEY_0 + nkl].key >= l->words[KEY_0 + nkl + 1].key) {
		    printf("index merge destroyed ordering of keys in node.\n");
		    exit(EXIT_FAILURE);
	    }
#endif	
	free_index_block(r);
	b->num_blks -= 1;
}

static int index_underflow(bplus_t b, unsigned d, blkp inode, unsigned *posp)
{
	/* time to restore invariant so all layers have >= ORDER/2 keys by combining nodes? */
	/* inode is not root, number of keys in inode < LHALF */
	blkp parent = b->path[d].node;
	unsigned pos = b->path[d].pos;
	unsigned nkp = b->path[d].num_keys;
	blkp rpeer = NULL;
	unsigned nkr;
	unsigned nki = num_keys(inode);
	if (pos < nkp) {
		rpeer = parent->words[FIELD_0 + pos + 1].child;
		nkr = num_keys(rpeer);
		/* if right peer has more keys than can be merged, rotate from right through parent */
		if (nki + nkr > ORDER - 2) {
			/* rotate rpeer key through its splitting key in parent */
			inode->words[KEY_0 + nki].key = parent->words[KEY_0 + pos].key;
			parent->words[KEY_0 + pos].key = rpeer->words[KEY_0].key;
			inode->words[FIELD_0 + nki + 1].value = rpeer->words[FIELD_0].value;
			wrdmove(rpeer->words + KEY_0, rpeer->words + KEY_0 + 1, nkr - 1);
			wrdmove(rpeer->words + FIELD_0, rpeer->words + FIELD_0 + 1, nkr);
			inode->words[HEADER].header.num_keys += 1;
			rpeer->words[HEADER].header.num_keys -= 1;
			return 0;
		}
		/* right peer can be merged */
	}
	if (pos > 0) {
		blkp lpeer = parent->words[FIELD_0 + pos - 1].child;
		unsigned nkl = num_keys(lpeer);
		/* else if left peer has more keys than can be merged, rotate from left through parent */
		if (nkl + nki > ORDER - 2) {
			wrdmove(inode->words + KEY_0 + 1, inode->words + KEY_0, nki);
			wrdmove(inode->words + FIELD_0 + 1, inode->words + FIELD_0, nki + 1);
			inode->words[KEY_0].key = parent->words[KEY_0 + pos - 1].key;
			parent->words[KEY_0 + pos - 1].key = lpeer->words[KEY_0 + nkl - 1].key;
			inode->words[FIELD_0].value = lpeer->words[FIELD_0 + nkl].value;
			inode->words[HEADER].header.num_keys += 1;
			lpeer->words[HEADER].header.num_keys -= 1;
			return 0;
		}
		/* merge into the left peer and delete inode */
		merge_index_nodes(b, lpeer, inode, parent->words[KEY_0 + pos - 1].key);
		/* parent[pos] to be removed recursively */
		*posp = pos;
	} else {
		/* else pos == 0, so merge right peer into inode. */
		merge_index_nodes(b, inode, rpeer, parent->words[KEY_0 + pos].key);
		/* parent[pos + 1] to be removed recursively */
		*posp = pos + 1;
	}
	return 1;
}

#ifdef CHECK_INVARIANTS
/* find rightmost key under inode at depth d */
static lkey_t rightmost_key(bplus_t b, blkp inode, unsigned d)
{
	return (d == b->depth) ? /* leaf node */
		inode->words[KEY_0 + num_keys(inode) - 1].key :
		rightmost_key(b, inode->words[FIELD_0 + num_keys(inode)].child, d + 1);
}
static lkey_t leftmost_key(bplus_t b, blkp inode, unsigned d)
{
	return (d == b->depth) ? /* leaf node */
		inode->words[KEY_0].key :
		leftmost_key(b, inode->words[FIELD_0].child, d + 1);
}
#endif

/* recurse over path removing node at pos */
static void shrink_index_ancestors(bplus_t b, unsigned d, unsigned pos)
{
	/* remove child from index node layer d at pos */
	/*
	 * NOTE: we never are called to delete the leftmost child of any index node
	 * from below. Only merges of index nodes manage splitting keys and their children.
	 */
	blkp inode = b->path[d].node;
	unsigned nk = b->path[d].num_keys;
	if (nk - pos > 0) { /* slide down key,child pairs after pos */
		wrdmove(inode->words + KEY_0 + pos - 1, inode->words + KEY_0 + pos, nk - pos);
		wrdmove(inode->words + FIELD_0 + pos, inode->words + FIELD_0 + pos + 1, nk - pos);
	}
	nk -= 1;
	inode->words[HEADER].header.num_keys = nk;
#ifdef CHECK_INVARIANTS /* after deleting child at key[pos-1]*/
	{
		for (unsigned i = pos - 1; i < nk; i++) {
			lkey_t key = inode->words[KEY_0 + i].key;
			blkp prev_child = inode->words[FIELD_0 + i].child;
			blkp child = inode->words[FIELD_0 + i + 1].child;
			lkey_t key_below = rightmost_key(b, prev_child, d + 1);
			lkey_t key_above = leftmost_key(b, child, d + 1);
			if (key_below >= key || key > key_above) {
				printf("Ordering failed at depth %u of %u: %lu ^< %lu ~<= %lu\n",
					d, b->depth, key_below, key, key_above);
				exit(EXIT_FAILURE);
			}
		}
	}
#endif
	if (d == 0) {
		/* this is the root, whose minimum size is 2 keys, if only one is left, delete root */
		if (nk == 0) {
			/*  delete this root here, promote the remaining child to root. */
			b->root = inode->words[FIELD_0].child;
			b->depth -= 1;
			free_index_block(inode);
			b->num_blks -= 1;
			if (b->depth == 0) {
				/* when tree has no index nodes, optionally clean up path */
				b->path_length = 0;
				free(b->path);
				b->path = NULL;
			}
		} else {
			/* root node has more than 1 remaining child, done */
		}
	} else if (nk < LHALF) {
		/* handle underflow by rotation or merge of inode and either peer */
		int merged = index_underflow(b, d - 1, inode, &pos);
		/* and recurse to parent (if merge, not rotate)*/
		if (merged)
			shrink_index_ancestors(b, d - 1, pos);
	} else {
		/* if non-root index node doesn't underflow, return */
	}
}

static void merge_leaf_nodes(bplus_t b, blkp l, blkp r)
{
	unsigned nkl = num_keys(l);
	unsigned nkr = num_keys(r);
	wrdmove(l->words + KEY_0 + nkl, r->words + KEY_0, nkr);
	wrdmove(l->words + FIELD_0 + nkl, r->words + FIELD_0, nkr);
	l->words[HEADER].header.num_keys += nkr;
	l->words[NEXT].leaf = r->words[NEXT].leaf;
	fix_cursor_merge(b, l, r, nkl);
	free_leaf_block(r);
	b->num_blks -= 1;
}

static void leaf_underflow(bplus_t b, blkp leaf)
{
	/* time to restore invariant so all layers have >= ORDER/2 keys by combining nodes? */
	/* leaf is not root, number of keys in leaf = LHALF - 1 */
	unsigned d = b->depth - 1;
	blkp parent = b->path[d].node;
	unsigned pos = b->path[d].pos;
	unsigned nk = b->path[d].num_keys;
	blkp rpeer = NULL;
	if (pos < nk) {
		rpeer = parent->words[FIELD_0 + pos + 1].child;
		/* if right peer has nkey > LHALF, rotate from right and fix split in parent */
		if (num_keys(rpeer) > LHALF) {
			leaf->words[KEY_0 + LHALF - 1].key = rpeer->words[KEY_0].key;
			leaf->words[FIELD_0 + LHALF - 1].value = rpeer->words[FIELD_0].value;
			wrdmove(rpeer->words + KEY_0, rpeer->words + KEY_0 + 1, num_keys(rpeer) - 1);
			wrdmove(rpeer->words + FIELD_0, rpeer->words + FIELD_0 + 1, num_keys(rpeer) - 1);
			leaf->words[HEADER].header.num_keys += 1;
			rpeer->words[HEADER].header.num_keys -= 1;
			parent->words[KEY_0 + pos].key = rpeer->words[KEY_0].key;
			fix_cursor_rotate_left(b, leaf, rpeer);
			return;
		}
	}
	if (pos > 0) {
		blkp lpeer = parent->words[FIELD_0 + pos - 1].child;
		/* else if left peer has nkey > LHALF, rotate from left, fixing split key in parent */
		if (num_keys(lpeer) > LHALF) {
			wrdmove(leaf->words + KEY_0 + 1, leaf->words + KEY_0, num_keys(leaf));
			wrdmove(leaf->words + FIELD_0 + 1, leaf->words + FIELD_0, num_keys(leaf));
			leaf->words[KEY_0].key = lpeer->words[KEY_0 + num_keys(lpeer) - 1].key;
			leaf->words[FIELD_0].value = lpeer->words[FIELD_0 +num_keys(lpeer) - 1].value;
			leaf->words[HEADER].header.num_keys += 1;
			lpeer->words[HEADER].header.num_keys -= 1;
			parent->words[KEY_0 + pos - 1].key = leaf->words[KEY_0].key;
			fix_cursor_rotate_right(b, lpeer, leaf);
			return;
		}
		/* merge with the left peer and delete leaf */
		merge_leaf_nodes(b, lpeer, leaf);
		shrink_index_ancestors(b, b->depth - 1, pos);
	} else {
		/* else pos == 0, so merge with right peer, which exists and was too small to borrow from */
		merge_leaf_nodes(b, leaf, rpeer);
		shrink_index_ancestors(b, b->depth - 1, pos + 1);
	}
}



enum bplus_error delete(bplus_t b, lkey_t k)
{
	enum bplus_error ok = path_reserved(b);
	if (ok == OK) {
		blkp leaf = find_leaf(b, k);
		unsigned nk = num_keys(leaf);
		unsigned i = scan_leaf_keys(leaf, k);
		if (i < nk && get_key(leaf, i) == k) {
			/* key k was found in leaf, remove record (key, value) */
			unsigned sfx_count = nk - i - 1;
			if (sfx_count != 0) {
				wrdmove(leaf->words + KEY_0 + i, leaf->words + KEY_0 + i + 1, sfx_count);
				wrdmove(leaf->words + FIELD_0 + i, leaf->words + FIELD_0 + i + 1, sfx_count);
			}
			leaf->words[HEADER].header.num_keys = nk - 1;
			b->num_recs -= 1;
			/* adjust all cursors pointing at leaf, at position i or after */
			for (bplus_cursor_t bc = b->cursor_list; bc != NULL; bc = bc->next) {
				if (bc->leaf == leaf) {
					if (bc->pos == i)
						bc->invalid = 1;
					else if (bc->pos > i)
						bc->pos -= 1;
				}
			}
			/* if new leaf size (nk - 1) < min size, handle this underflow */
			if (b->depth > 0 && nk <= LHALF)
				leaf_underflow(b, leaf);
		} else ok = NOTFOUND;
	}
	return ok;
}

void enumerate(bplus_t b, void (*f)(lkey_t k, value_t v))
{
	for (blkp bk = b->leaves; bk != NULL; bk = next_leaf(bk)) {
		for (unsigned i = 0; i < bk->words[HEADER].header.num_keys; i++) {
			f(get_key(bk, i), get_value(bk, i));
		}
	}
}

bplus_cursor_t first_record(bplus_t b)
{
	bplus_cursor_t c = make_bplus_cursor(b, b->leaves, 0);
	return c;
}

enum bplus_error next_record(bplus_cursor_t c)
{
	blkp l = c->leaf;
	if (c->invalid)
		c->invalid = 0;
	else
		c->pos += 1;
	if (num_keys(l) <= c->pos) {
		l = next_leaf(l);
		c->leaf = l;
		c->pos = 0;
		return (l != NULL) ? OK : NOTFOUND;
	}
	return OK;
}

bplus_cursor_t find_record(bplus_t b, lkey_t k)
{
	bplus_cursor_t c = NULL;	
	if (b->root != NULL) {
		if (path_reserved(b) == OK) {
			blkp leaf = find_leaf(b, k);
			/* scan leaf keys for match */
			unsigned i = scan_leaf_keys(leaf, k);
			/* i is the first key >= k */
			c = make_bplus_cursor(b, leaf, i);
		}
	}
	return c;
}

enum bplus_error get_record(bplus_cursor_t c, lkey_t *k, value_t *v)
{
	blkp l = c->leaf;
	unsigned p = c->pos;
	if (!c->invalid && l != NULL && num_keys(l) > p) {
		*k = get_key(l, p);
		*v = l->words[FIELD_0 + p].value;
		return OK;
	}
	return NOTFOUND;
}

enum bplus_error update_record(bplus_cursor_t c, value_t v)
{
	blkp l = c->leaf;
	unsigned p = c->pos;
	if (!c->invalid && l != NULL && num_keys(l) > p) {
		l->words[FIELD_0 + p].value = v;
		return OK;
	}
	return NOTFOUND;
}

void free_cursor(bplus_cursor_t c)
{
	bplus_t b = c->tree;
	/* if tree still exists remove cursor from tree's cursor list */
	if (b != NULL) {
		bplus_cursor_t *l = &b->cursor_list;
		while (*l != NULL) {
			if (*l == c) {
				*l = c->next;
				break;
			}
			l = &((*l)->next);
		}
		/* *l should NOT be NULL, or an invariant broke */
	}
	free(c);
	b->num_crsrs -= 1;
}

bplus_t get_tree(bplus_cursor_t c)
{
	return c->tree;
}
