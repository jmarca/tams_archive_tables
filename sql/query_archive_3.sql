COPY
   (with
     a as ( select * from archive.signaturearchive_201702_4002 limit  10   )
   select * from a)
TO STDOUT;
