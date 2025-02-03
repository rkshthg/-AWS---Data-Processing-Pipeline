create external table datacraft_retail.stage_table (
RetailTxnData_InvoiceNo string,
RetailTxnData_InvoiceDateTime string,
RetailTxnData_StoreNo string,
RetailTxnData_StorePos string,
RetailTxnData_Region string,
RetailTxnData_StoreCountry string,
RetailTxnData_LineItem_StockCode string,
RetailTxnData_LineItem_Description string,
RetailTxnData_LineItem_Quantity string,
RetailTxnData_LineItem_UnitPrice string,
RetailTxnData_LineItem_SalePrice string,
RetailTxnData_Customer_CustomerID string,
RetailTxnData_Customer_EmailId string,
RetailTxnData_Customer_ContactNo string,
RetailTxnData_Customer_FirstName string,
RetailTxnData_Customer_MiddleName string,
RetailTxnData_Customer_LastName string,
RetailTxnData_Customer_Address string,
RetailTxnData_Customer_State string,
RetailTxnData_Customer_Country string,
RetailTxnData_Customer_ZipCode string,
RetailTxnData_Discount_DiscountCode string,
RetailTxnData_SourceTimestamp string
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://datacraft-retail-dataset/data/stage/data/'
TBLPROPERTIES('classification'='parquet');