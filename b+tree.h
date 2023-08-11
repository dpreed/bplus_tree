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

/* External interface for the simple b+ tree library */

#ifndef _BPLUSTREE_H_
#define _BPLUSTREE_H_

typedef unsigned long lkey_t;
typedef unsigned long value_t;

typedef struct bplus * bplus_t;
typedef struct bplus_cursor *bplus_cursor_t;

/* Library manages bplus tree objects and cursor objects */
enum bplus_error {
        OK = 0,
        NOTFOUND,
        NOMEM,
        MAX_BPLUS_ERROR,
};

/* create new empty bplus tree */
bplus_t new_bplus_tree(void);

/*
 * give a valid bplus tree, destroys it and invalidates
 * all associated cursors so using them will cause an error.
 */
void free_bplus_tree(bplus_t b);

/*
 * given a key, find the associated value.
 * returns OK if key present, setting *v to value
 * or NOTFOUND.
 */
enum bplus_error find(bplus_t b, lkey_t k, value_t *v);


/*
 * given a key and value pair, insert record into tree
 * If key exists, update the value in the record to v.
 * If any active cursors referred to a deleted key == k, they
 * will refer to the inserted record.
 * Returns OK if insert succeeded, NOMEM if insufficient memory
 * to hold the new record
 */
enum bplus_error insert(bplus_t b, lkey_t k, value_t v);

/* 
 * given a key, delete corresponding record, and if there
 * are any cursors pointing at the record, mark the record
 * as missing. but preserve the cursor so next_record() works.
 * if key not present return NOTFOUND, otherwise OK.
 */
enum bplus_error delete(bplus_t b, lkey_t k);

/*
 * enumerate all records in tree, in key order, calling the
 * specified function with key and value.
 */
void enumerate(bplus_t b, void (*f)(lkey_t k, value_t v));

/* 
 * create a cursor on the tree, at the first record
 * If allocation fails, return NULL.
 */
bplus_cursor_t first_record(bplus_t b);

/* 
 * position the cursor at the next record in tree after cursor record.
 * Returns OK if there is a next record, NOTFOUND if at end.
 * (if the current record has been deleted, it still returns the next remaining
 * record after that record)
 */
enum bplus_error next_record(bplus_cursor_t c);

/*
 * get a cursor to the record in the tree with the lowest key >= k
 * If allocation fails, return NULL.
 */
bplus_cursor_t find_record(bplus_t b, lkey_t k);

/*
 * get the key and value of the record at cursor, if present, and returns OK
 * returns NOTFOUND if deleted */
enum bplus_error get_record(bplus_cursor_t c, lkey_t *k, value_t *v);

/* set the value of the record at cursor returning OK, returns NOTFOUND if deleted. */
enum bplus_error update_record(bplus_cursor_t c, value_t v);

/* free the cursor and stop tracking it */
void free_cursor(bplus_cursor_t c);

/* for convenience, get the tree the cursor is enumerating. Returns NULL if tree has been deleted. */
bplus_t get_tree(bplus_cursor_t c);

/* Currently active storage statistics */
void get_active_storage(bplus_t b, unsigned long *num_records, unsigned long *num_blocks, unsigned long *num_cursors);

#endif