/*
 * Simple implementation of B+ Tree in C.
 * Copyright (c) 2020-2023 David P. Reed
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

/* simple b+ tree implementation to emulate in-memory DBMS behavior */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <libgen.h>
#include <string.h>
#include <locale.h>

#include "b+tree.h"

/* build a b+ tree to fill memory simulating sharding of keys across children  */

int main(int argc, char *argv[])
{
	char *cmd_name = "XX";
	size_t ngigs = sysconf(_SC_AVPHYS_PAGES) >> 18; // 2**18 pages is 1 GiB
	size_t nb = ((ngigs - 3) << 30) & ~0xFFFUL; /* Reserve 3 GB for overhead */

	bplus_t bpt = new_bplus_tree();
	unsigned long nrecs = 0;
	unsigned long nblocks = 0;
	unsigned long ncursors = 0;
	unsigned long used = 0;
	lkey_t key;
	value_t value;
	bplus_cursor_t cursor;
	enum bplus_error ok;
	char randstate[32];
	unsigned long count = 0;
	unsigned long found = 0;
	unsigned long notfound = 0;

	setlocale(LC_ALL, "");


	cmd_name = basename(argv[0]);

	printf("System has %'ld gigabytes (so filling %'ld bytes) of RAM\n", ngigs, nb);

	/*
	 * initialize pseudo-random number generator with same seed
	 * in each child process. This simulates sharding of the index
	 * by key value, maximizing concurrency without interlocking.
         */
	initstate(314159, randstate, sizeof(randstate));

	/* generate key, value pairs */
	do {
		key = random();
		value = random();

		/* insert key value pairs */
		ok = insert(bpt, key, value);
		if (ok != OK) {
			fprintf(stderr, "Error %u\n", ok);
			return 0;
		}
		count += 1;
		/* until tree fills this process's share of physical storage */
		get_active_storage(bpt, &nrecs, &nblocks, &ncursors);
		used = nblocks << 12; /* each block takes 1 << 12 bytes */
		if (nrecs % 1000 == 0)
			printf("%'ld records inserted, %'lu bytes used\n", nrecs, used);
	} while (used < nb);

	printf("Inserted %'lu records\n", count);

	initstate(314159, randstate, sizeof(randstate));

	printf("Looking up %'lu records\n", count);
	for (unsigned long i = 0; i < count; i++) {
		key = random();
		value = random();
		ok = find(bpt, key, &value);
		switch (ok) {
		case OK:
			found += 1;
			break;
		default:
			notfound += 1;
		}
	}
	printf("Found %'lu records, didn't find %'lu\n",
	       found, notfound);


	count = 0;
	cursor = first_record(bpt);
	if (cursor != NULL) {
		while (get_record(cursor, &key, &value) == OK) {
			if (delete(bpt, key) != OK) {
				fprintf(stderr,
					"%s: BUG: Key %lu not found\n", cmd_name, key);
				break;
			}
			count += 1;
			if (next_record(cursor) != OK)
				break;
		}
		free_cursor(cursor);
	}
	
	printf("Removed %'lu records in order using cursor\n",
	       count);
	
	free_bplus_tree(bpt);
	return 0;
}
