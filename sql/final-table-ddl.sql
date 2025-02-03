CREATE EXTERNAL TABLE datacraft_retail.curated_table
LOCATION 's3://datacraft-retail-dataset/data/curated/data/'
TBLPROPERTIES ('table_type' = 'DELTA')