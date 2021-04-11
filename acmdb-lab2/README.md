# SimpleDB Lab-2 Doc

- Exercise 1: we implement the LRU eviction policy, achieved by a `LinkedList`.
  
- Exercise 2: the `findLeafPage` is implemented by recursively comparing the keys in internal pages with the given value, and calling the child trees.

- Exercise 3: for insertion, the crucial thing is splitting. When splitting, we first create a new page into which half of entries in the full page will be poured. Then we push up some necessary information to the parent page, and finally maintain the tree structure. 