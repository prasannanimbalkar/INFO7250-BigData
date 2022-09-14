DATA = LOAD '/MainData' USING PigStorage(',') AS (Invoice:chararray,StockCode:chararray,Description:chararray,Quantity:int,InvoiceDate:chararray,Price:int,CustomerID:int,Country:int);
GEN = FOREACH DATA GENERATE CustomerID as custid, Invoice;
counter = GROUP GEN BY (custid, Invoice);
row_count = FOREACH counter GENERATE FLATTEN(GEN), COUNT(GEN) as count;
x= DISTINCT row_count;
somethingDoing = GROUP x BY custid;
STORE somethingDoing INTO '/PIG1' USING PigStorage(',');





























exec hdfs://localhost:9000/