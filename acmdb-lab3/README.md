# SimpleDB Lab-2 Doc

- Exercise 1: we implement the LRU eviction policy, achieved by a `LinkedList`.
- Exercise 2: the `findLeafPage` is implemented by recursively comparing the keys in internal pages with the given value, and calling the child trees.
- Exercise 3: for insertion, the crucial thing is splitting. When splitting, we first create a new page into which half of entries in the full page will be poured. Then we push up some necessary information to the parent page, and finally maintain the tree structure.
- Exercise 4: for deletion, when stealing for an internal page, we first pull down the parent's entry, and then "rotate" needed entries. When stealing for a leaf page, we simply move the entries.
- Exercise 5: analogous to exercise 4. It is worth paying attention to whether we should first pull down the parent' entry and then move the needed entries.

I did not make any essential changes to the API in this lab.

I spent approximately two days on it. I do not think there is any trouble here.

