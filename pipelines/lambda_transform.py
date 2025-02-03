import json
import base64
from flatsplode import flatsplode

def lambda_handler(event, context):
    """
    AWS Lambda handler to process Kinesis event records, decode them, 
    and store in S3 and Firehose.

    Args:
        event (dict): Event payload from Kinesis.
        context (LambdaContext): Runtime information.
    """
    print(event)
    output = []
    for record in event['records']:
        print("Record ID:",record['recordId'])
        payload = base64.b64decode(record['data']).decode('utf-8')
        print("Incoming Payload:\n", payload)

        # Do custom processing on the payload here
        data = json.loads(payload)
        print("Data:\n", data)

        flat_list = list(flatsplode(data, "_"))
        final_msg = ""
        for msg in flat_list:
            final_msg = final_msg + json.dumps(msg)

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(final_msg.encode('utf-8'))
        }
        print("Outgoing Response:\n", output_record)
        output.append(output_record)

    return {'records': output}
