# simpledb

A toy RDBMS implemented in Java.
Based on SimpleDB of ["Database Design and Implementation"](https://link.springer.com/book/10.1007/978-3-030-33836-7).
Original code of SimpleDB is [here](http://www.cs.bc.edu/~sciore/simpledb/).

## how it works

run `SimpleIJ.java`

```
Connect >
test

SQL> create table T1(A int, B varchar(9));
SQL> insert into T1(A, B) values (1, 'b value');
SQL> insert into T1(A, B) values (2, 'b value 2');
SQL> select A, B from T1 where A = 1;
      a         b
-----------------
      1   b value
```

## working on

- implement row locking
  - ["Database Design and Implementation" の SimpleDB に行ロックを実装](https://zenn.dev/hmarui66/scraps/d0f20edd53046b)
- implement B-Tree
  - https://github.com/hmarui66/simpledb/pull/3

## notes

- JDK 17 で動作(するはず)
- warning 大量なので直したい
 
## link

- [Database Design and Implementation Second Edition](https://link.springer.com/book/10.1007/978-3-030-33836-7)
- [読書メモ: Database Design and Implementation](https://zenn.dev/hmarui66/scraps/850df4edc50c58)
