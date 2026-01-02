from fastapi import APIRouter, HTTPException, status
from app.models import IngestRequest, IngestResponse
from pipeline.queue import IngestionQueue, QueueFullError

router = APIRouter()
ingestion_queue = IngestionQueue(max_size=1000)

async def accept_records(records: list[dict]) -> None:
    '''
    Acceptance boundary for ingestion pipeline.

    Responsibilities:
    - Enforce backpressure
    - Enqueue records into bounded queue
    '''

    await ingestion_queue.enqueue(records)

@router.post("/ingest", response_model=IngestResponse)
async def ingest(request: IngestRequest) -> IngestResponse:
    '''
    Accepts raw JSON records and forwards them into the ingestion pipeline.
    '''

    records = request.records()

    try:
        await accept_records(records)
    except QueueFullError:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="ingestion queue is full",
        )

    return IngestResponse(
        accepted_count=len(records),
        message=f'successfully ingested {len(records)} records.'
    )