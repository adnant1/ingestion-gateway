import asyncio
from typing import List
from pipeline.queue import IngestionQueue
from sinks.base import Sink

class BatchWorker:
    '''
    Background worker that consumes records from the ingestion queue
    and emits batches.

    Responsibilities:
    - Pull records from queue
    - Accumulate records into batch
    - Flush batch by size or time
    '''

    def __init__(
            self,
            queue: IngestionQueue,
            batch_size: int,
            flush_interval: float,
            sink: Sink
    ):
        self.queue = queue
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.sink = sink

        # Internal state
        self.current_batch: List[dict] = []
    
    async def run(self) -> None:
        '''
        Main worker loop.

        Runs indefinitely until cancelled 
        '''

        last_flush = asyncio.get_event_loop().time()
        while True:
            try:
                record = await asyncio.wait_for(
                    self.queue.dequeue(),
                    timeout=self.flush_interval
                )
                self.current_batch.append(record)

                # Flush by size
                if len(self.current_batch) >= self.batch_size:
                    await self._flush()
                    last_flush = asyncio.get_event_loop().time()
            except asyncio.TimeoutError:
                # Flush by time
                current_time = asyncio.get_event_loop().time()
                if current_time - last_flush >= self.flush_interval:
                    if self.current_batch:
                        await self._flush()
                    last_flush = current_time

    async def _flush(self) -> None:
        '''
        Deliver the current batch to the sink.

        Semantics:
        - No-op if batch is empty
        - Sink is called with a full batch
        - Batch is cleared only after successful write
        '''

        if not self.current_batch:
            return
        
        batch = self.current_batch
        self.current_batch = []
        await self.sink.write_batch(batch)