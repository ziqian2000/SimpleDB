# SimpleDB Lab-1 Doc

- Exercise 1: we utilize an array of `TDItem` to implement the `TupleDesc`. 
- Exercise 2: we use 4 `HashMap` to manage all tables and their properties, including its name, the name of its primal key, etc. 
- Exercise 3: we use a `HashMap` that serves as a link between `PageId` and `Page`. 
- Exercise 4: this exercise renders me familiar with Heap structure in SimpleDB. We provide the `TupleIterator` that iterates on each page, which will come in handy in the following exercises.
- Exercise 5: we provide a `HeapFileIterator` to iterate tuples of a file. This iterator proceeds by calling the iterators on each page.
- Exercise 6: to scan a specific file, we simply call the iterator of the underlying file. Then everything will be fine.

I did not make any essential changes to API since it is just a fundamental and straightforward project.

I spent approximately two days on it. I do not think there is any trouble in the first lab.

