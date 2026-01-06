from sinks.base import Sink
import json

class FileSink(Sink):
    '''
    Sink that appends records to a gile as newline-delimited JSON.
    '''

    def __init__(self, path: str):
        self.path = path

    async def write_batch(self, records: list[dict]) -> None:
        with open(self.path, 'a') as f:
            for record in records:
                f.write(json.dumps(record) + '\n')