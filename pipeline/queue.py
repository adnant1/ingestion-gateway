from asyncio import Queue

class IngestionQueue:
    '''
    Bounded async queue for ingestion records.

    Responsibilities:
    - Enforce max capacity
    - Provide async enqueue operations
    - Surface backpressure explicitly
    '''

    def __init__(self, max_size: int):
        self.queue = Queue(maxsize=max_size)

    async def enqueue(self, records: list[dict]) -> None:
        '''
        Attempt to enqueue records into the queue.

        Semantics:
        - If capacity is available, enqueue all records
        - If capacity is exhausted:
            - Raise a backpressure error
            - Do not partially enqueue records
        '''

        if self.size() + len(records) > self.queue.maxsize:
            raise QueueFullError('ingestion queue capacity exhausted')

        for record in records:
            await self.queue.put(record)
    
    async def dequeue(self) -> dict:
        '''
        Remove and return a single record from the queue.

        Semantics:
        - Blocks until a record is available
        - Safe for background consumers
        '''

        return await self.queue.get()
        
    def size(self) -> int:
        '''
        Current number of records in the queue.
        '''

        return self.queue.qsize()
    
class QueueFullError(Exception):
    '''
    Raised when ingestion queue capacity is exhausted.
    '''

    pass
    
    