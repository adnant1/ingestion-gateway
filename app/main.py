import asyncio
import os
import signal
from contextlib import asynccontextmanager
from dotenv import load_dotenv

from fastapi import FastAPI

from app.api import router
from pipeline.queue import IngestionQueue
from pipeline.batch_worker import BatchWorker
from pipeline.retry import RetryPolicy
from sinks.base import Sink
from sinks.terminal import TerminalSink
from sinks.file import FileSink

load_dotenv()

def build_sink() -> Sink:
    '''
    Create the sink from environment configuration.
    '''

    sink_type = os.getenv('SINK_TYPE', 'terminal').lower()

    if sink_type == 'terminal':
        return TerminalSink()
    elif sink_type == 'file':
        path = os.getenv('FILE_SINK_PATH')
        if not path:
            raise ValueError('FILE_SINK_PATH environment variable must be set for file sink')

        return FileSink(path=path)
    else:
        raise ValueError(f'unknown sink type: {sink_type}. Valid options: terminal, file')
    
def build_dql_sink() -> Sink:
    '''
    Create the DLQ sink from environment configuration.
    '''

    dlq_path = os.getenv('DLQ_PATH')
    if not dlq_path:
        raise ValueError('DLQ_PATH environment variable must be set for DLQ sink')

    return FileSink(path=dlq_path)

@asynccontextmanager
async def lifespan(app: FastAPI):
    '''
    Application lifecycle manager.

    Startup:
    - Build core components
    - Start background workers

    Shutdown:
    - Signal background workers to stop
    - Flush pending data
    - Wait for clean exit
    '''

    # Startup
    stop_event = asyncio.Event()

    sink: Sink = build_sink()
    dlq_sink: Sink = build_dql_sink()
    ingestion_queue = IngestionQueue(max_size=1000)
    retry_policy = RetryPolicy(
        max_attempts=3,
        base_delay=0.5
    )
    batch_worker = BatchWorker(
        queue=ingestion_queue,
        batch_size=100,
        flush_interval=5.0,
        sink=sink,
        dlq_sink=dlq_sink,
        retry_policy=retry_policy
    )

    worker_task = asyncio.create_task(batch_worker.run(stop_event))

    app.state.stop_event = stop_event
    app.state.ingestion_queue = ingestion_queue
    app.state.batch_worker = batch_worker
    app.state.worker_task = worker_task

    # Uvicorn already triggers FastAPI shutdown on SIGTERM/SIGINT, but
    # we also want to notify our worker to stop.

    loop = asyncio.get_running_loop()
    def _handle_shutdown():
        loop.call_soon_threadsafe(stop_event.set)
    
    signal.signal(signal.SIGTERM, lambda s, f: _handle_shutdown())
    signal.signal(signal.SIGINT, lambda s, f: _handle_shutdown())

    yield

    # Shutdown
    stop_event.set()
    await batch_worker.flush_now()

    try:
        await worker_task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="multi-storage ingestion gateway",
    description="async ingestion gateway for multiple storage backends",
    lifespan=lifespan
)

app.include_router(router)