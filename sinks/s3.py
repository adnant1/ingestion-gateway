import json
import time
import boto3

from sinks.base import Sink


class S3Sink(Sink):
    '''
    Sink that writes batches to Amazon S3.

    Each batch is written as a single object.
    '''

    def __init__(
            self,
            bucket: str,
            prefix: str = 'ingestion/',
            region: str | None = None,
    ):
        self.bucket = bucket
        self.prefix = prefix.rstrip('/') + '/'

        self.s3 = boto3.client('s3', region_name=region)
    
    async def write_batch(self, records: list[dict]) -> None:
        '''
        Persist a batch of records to S3.

        Semantics:
        - One batch -> one object
        - Object write must be atomic
        - Raise exception on failure
        '''

        timestamp = int(time.time() * 1000)
        object_key = f'{self.prefix}batch_{timestamp}.ndjson'

        ndjson_data = '\n'.join(json.dumps(record) for record in records) + '\n'

        self.s3.put_object(
            Bucket=self.bucket,
            Key=object_key,
            Body=ndjson_data.encode('utf-8'),
        )