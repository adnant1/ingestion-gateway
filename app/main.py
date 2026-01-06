import asyncio
import os
import signal
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import router
from pipeline.queue import IngestionQueue
from pipeline.batch_worker import BatchWorker
from sinks.terminal import TerminalSink
from sinks.file import FileSink

def build_sink():
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


app = FastAPI(
    title="multi-storage ingestion gateway",
    description="async ingestion gateway for multiple storage backends",
)

app.include_router(router)