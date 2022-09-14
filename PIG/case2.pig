DATA = LOAD '/MainData' USING PigStorage(',') AS (Invoice:chararray,StockCode:chararray,Description:chararray,Quantity:double,InvoiceDate:chararray,Price:double,CustomerID:int,Country:int);
DP = FOREACH DATA GENERATE  Price as price,Quantity as quantity, FLATTEN( STRSPLIT (InvoiceDate, ' ', 2 )) as dt;
count = FOREACH DP GENERATE dt, price*quantity AS total;
GROUP_DP = GROUP count by dt;
res = FOREACH GROUP_DP GENERATE group, SUM(count.total);
STORE res INTO '/PIG2' USING PigStorage(',');