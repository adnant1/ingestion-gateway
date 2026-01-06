import json
from sinks.base import Sink

class DLQSink(Sink):
    '''
    Sink for permanently failed records.

    Writes records to a durable, append-only destination.
    '''

    def __init__(self, dlq_path: str):
        self.dlq_path = dlq_path

    async def write_batch(self, records: list[dict]) -> None:
        '''
        Persist failed records to the DLQ.

        Semantics:
        - Must not drop records
        - Must not partially write silently
        - If this fails, let the exception propagate
        '''

        with open(self.dlq_path, 'a') as f:
            for record in records:
                f.write(json.dumps(record) + '\n')
