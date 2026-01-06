from sinks.base import Sink

class TerminalSink(Sink):
    '''
    Sink that writes records to the terminal (stdout).

    Useful for development and testing.
    '''

    async def write_batch(self, records: list[dict]) -> None:
        for record in records:
            print(record)