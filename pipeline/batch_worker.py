import asyncio
from typing import List
from pipeline.errors import DeliveryError
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
            sink: Sink,
            dlq_sink: Sink,
            retry_policy,
    ):
        self.queue = queue
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.sink = sink
        self.dlq_sink = dlq_sink
        self.retry_policy = retry_policy

        # Internal state
        self.current_batch: List[dict] = []
    
    async def run(self, stop_event: asyncio.Event) -> None:
        '''
        Main worker loop.

        Runs indefinitely until cancelled 
        '''

        last_flush = asyncio.get_event_loop().time()
        while not stop_event.is_set():
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
            except asyncio.CancelledError:
                await self.flush_now()
                raise
                
        await self.flush_now() # Flush remaining records on stop
    
    async def flush_now(self) -> None:
        '''
        Immediately flush the current batch.
        '''

        await self._flush()

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
        
        batch = list(self.current_batch)

        try:
            await self.retry_policy.execute(
                lambda: self.sink.write_batch(batch)
            )

            self.current_batch.clear()
        except DeliveryError:
            await self.dlq_sink.write_batch(batch)
            self.current_batch.clear()