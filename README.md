# bplus_tree
Simple b+ tree with test program

Basically, this is an in-memory B+ tree that I wrote based using the standard algorithms.
While B+ trees are common in DBMS's most open source B+ trees are entangled with lots of file access code.

It is optimized so that all nodes are 4K pages for efficient virtual memory organization.
The keys are unsigned 64 bit integers and the values can be any 64 bit value. Thus integers or doubles or pointer values can be used directly.

If you want to use a different data type for a key, the code should be easy to modify to make the keys into pointers to the actual key, and adding a comparison function to replace numeric comparison.

No guarantees that there are no bugs, but I did extensive testing on lots of corner cases.

The main.c program just does a straightforward build of a tree with random, filling all available memory, looking up all the keys, and then deleting the tree. It also serves as an example.

Since it is under MIT license, anyone may take it, modify it, use it. I'd appreciate any comments or suggested improvements.
