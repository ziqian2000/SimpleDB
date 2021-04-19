# SimpleDB Lab-3 Doc

- Exercise 1: I implement those operators following their definitions. For `join`, I use the nested loops. Basically, there is nothing to say.
- Exercise 2: I apply the given aggregation to tuples one by one and save the results. There is not much to say.
- Exercise 3: to insert, I simply find an empty slot and then place the tuple there. To delete, I first locate the tuple using `RecordId` and then remove this tuple.  
- Exercise 4: I believe something mysterious will happen if I do insertion and deletion while going through the given iterator. Therefore, I just store all data produced by the iterator into an array before doing any modification (insertion or deletion).

I did not make any essential changes to the API in this lab.

I spent approximately three days on it. I do not find any extreme trouble here.

