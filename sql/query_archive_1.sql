
COPY
   (with
     a as ( select * from archive.signaturearchive_1_2016 where detstaid= 10004 limit  10   ),
     b as ( select * from archive.signaturearchive_1_2016 where detstaid=    46 limit  10   ),
     c as ( select * from archive.signaturearchive_1_2016 where detstaid=  6003 limit  10   ),
     d as ( select * from archive.signaturearchive_1_2016 where detstaid= 10007 limit  10   ),
     e as ( select * from archive.signaturearchive_1_2016 where detstaid=  7001 limit  10   ),
     f as ( select * from archive.signaturearchive_1_2016 where detstaid=  6001 limit  10   ),
     g as ( select * from archive.signaturearchive_1_2016 where detstaid= 10003 limit  10   ),
     h as ( select * from archive.signaturearchive_1_2016 where detstaid=  7003 limit  10   ),
     i as ( select * from archive.signaturearchive_1_2016 where detstaid= 10005 limit  10   ),
     j as ( select * from archive.signaturearchive_1_2016 where detstaid= 10002 limit  10   ),
     k as ( select * from archive.signaturearchive_1_2016 where detstaid=  6007 limit  10   ),
     l as ( select * from archive.signaturearchive_1_2016 where detstaid=  6004 limit  10   ),
     m as ( select * from archive.signaturearchive_1_2016 where detstaid=  6002 limit  10   ),
     n as ( select * from archive.signaturearchive_1_2016 where detstaid= 10001 limit  10   ),
     o as ( select * from archive.signaturearchive_1_2016 where detstaid= 10006 limit  10   ),
     p as ( select * from archive.signaturearchive_1_2016 where detstaid=  6005 limit  10   ),
     q as ( select * from archive.signaturearchive_1_2016 where detstaid=   113 limit  10   ),
     r as ( select * from archive.signaturearchive_1_2016 where detstaid=  7002 limit  10   )
   select * from a
    union select * from b
    union select * from c
    union select * from d
    union select * from e
    union select * from f
    union select * from g
    union select * from h
    union select * from i
    union select * from j
    union select * from k
    union select * from l
    union select * from m
    union select * from n
    union select * from o
    union select * from p
    union select * from q
    union select * from r )
TO STDOUT;
