import json
import boto3
import base64

def lambda_handler(event, context):
    """
    AWS Lambda handler to process Kinesis event records, decode them, 
    and store in S3 and Firehose.

    Args:
        event (dict): Event payload from Kinesis.
        context (LambdaContext): Runtime information.
    """
    print(event)
    for record in event["Records"]:
        #Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        print("Decoded payload: " + payload)
        
        data = json.loads(payload)
        print(type(data))

        # Generate S3 archive file path key
        invoiceno = data["RetailTxnData"]["InvoiceNo"]
        storeno = data["RetailTxnData"]["StoreNo"]
        invoiceprocess_date = data["RetailTxnData"]["InvoiceDateTime"][0:10]
        region = data["RetailTxnData"]["Region"].replace(" ", "")
        json_file_key = (
                f'raw/Region={region}/StoreNo={storeno}/'
                f'invoiceprocess_date={invoiceprocess_date}/'
                f'retail-dataset-{region}-{storeno}-{invoiceno}.json'
            )
        print(json_file_key)

        # Put the transaction into S3://bucket/archive
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket='datacraft-retail-dataset',
            Key=json_file_key,
            Body=payload
        )

        firehose_client = boto3.client('firehose', region_name='us-east-1')
        record = {"Data": payload}
        response = firehose_client.put_record(
            DeliveryStreamName='retail-delivery-stream',
            Record=record
        )
        print(f'\nFirehose Response:\n {response}\n')



