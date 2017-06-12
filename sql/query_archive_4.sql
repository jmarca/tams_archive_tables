COPY
   (with
     a as ( select * from archive.signaturearchive_201702_6002 limit  10   )
   select * from a)
TO STDOUT;
