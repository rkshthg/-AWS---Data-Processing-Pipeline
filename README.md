# Data Processing Pipeline on AWS

## Overview

This project implements a highly scalable and automated data processing pipeline on AWS, designed to efficiently handle large volumes of JSON data. The pipeline begins by reading JSON data files stored in an Amazon S3 bucket and then streams their contents through AWS Lambda and Kinesis services for real-time processing. The data undergoes transformation, including flattening and restructuring, to ensure optimal usability. Finally, the refined data is written in Delta format, leveraging the power of Apache Spark and Delta Lake for improved querying, analytics, and storage efficiency.

## Data Source

The data used for demonstration is generated from [Mockaroo](https://www.mockaroo.com/) in JSON format and contains simulated sales data for a hardware store. This dataset includes various attributes such as product names, categories, prices, transaction dates, and customer details, providing a realistic representation of retail transactions within a hardware business. The mock data allows for extensive testing and validation of the data pipeline without relying on actual production data.

### Data Schema

[Data Schema file](/data/mock-data-schema.json)

## Architecture

1. **Data Ingestion**

   - JSON data files are stored in an S3 bucket.
   - Kinesis Data Stream picks up the files and streams them to an AWS Lambda function.

2. **Raw Data Storage**

   - The processed files from Lambda are written to a **raw** folder in S3.
   - The same files are also streamed to **Kinesis Data Firehose**.

3. **Data Transformation**

   - Kinesis Data Firehose sends JSON data to a transformer AWS Lambda function.
   - The transformer Lambda flattens the JSON data and writes it to a **staging** folder in S3 in microbatches.

4. **Data Processing in EMR**

   - The data from the **staging** folder is processed using PySpark on an Amazon EMR cluster.
   - The transformed data is written to a **Delta format** in another S3 folder for efficient querying and analytics.

![Data Processing Pipeline on AWS Architecture](/docs/architecture.jpg)

## Monitoring & Logging
- AWS CloudWatch service is used for monitoring and active logging.

## Technologies Used

- **Amazon S3** – Cloud storage for raw, staged, and transformed data.
- **AWS Lambda** – Serverless computing for processing and transforming data.
- **Amazon Kinesis Data Stream** – Real-time data streaming.
- **Amazon Kinesis Data Firehose** – Data delivery and transformation service.
- **Amazon EMR** – Managed big data processing service for Apache Spark and Hadoop.
- **Apache Spark (PySpark)** – Distributed data processing on EMR.
- **Delta Lake** – Optimized storage layer for big data.
- **AWS Cloudwatch** - Monitoring and Logging

## Project Structure
```
project_root/
│-- config/              # Configuration files (YAML, JSON, ENV)
      |-- cloudformation-config.json
      │-- .env           # Environment variables
│-- data/                # Raw and processed data
   |-- source/
   |-- raw/
   |-- processed/
   |-- final/
│-- dags/                # Airflow DAGs for pipeline automation
│-- notebooks/           # Jupyter notebooks for analysis
   |-- load.ipynb
│-- pipelines/           # ETL pipeline scripts (extract.py, transform.py, load.py)
   |-- extract.py
   |-- lambda_process.py
   |-- lambda_transform.py
   |-- load.py
│-- scripts/             # Utility and helper scripts
│-- docs/                # Documentation and API references
   |-- architecture.jpg
|-- sql/                 # SQL scripts for DDLs and DMLs
   |-- stage-table-ddl.sql
   |-- final-table-ddl.sql
│-- README.md            # Project documentation
```

## Setup Instructions

1. **S3 Configuration**

   - Create an S3 bucket with separate folders for `raw`, `staging`, and `delta` data.

2. **Kinesis Setup**

   - Configure a **Kinesis Data Stream** to stream JSON files.
   - Set up a **Kinesis Data Firehose** to send transformed data to Lambda.

3. **Lambda Functions**

   - Deploy an AWS Lambda function to process Kinesis streams and write raw data.
   - Deploy a transformer Lambda to flatten JSON and write to the staging folder.

4. **EMR Cluster**

   - Configure an Amazon EMR cluster with PySpark.
   - Implement a PySpark job to transform the staged data and write in Delta format.

## Usage

- Upload JSON files to the S3 bucket.
- The pipeline will automatically process and store transformed data in Delta format.
- Use Amazon Athena or Apache Spark to analyze the processed data efficiently.

## Future Enhancements

- Automate the load function using orchestration tools as in architecture diagram.
- Implement schema evolution and validation using AWS Glue.
- Add monitoring and alerting with CloudWatch and AWS Lambda logs.
- Optimize PySpark jobs for better performance and cost efficiency.

## License

This project is open-source. You may use, modify, and distribute it under the [MIT License](LICENSE).
