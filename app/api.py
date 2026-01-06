from fastapi import APIRouter, HTTPException, Request, status
from app.models import IngestRequest, IngestResponse
from pipeline.queue import QueueFullError

router = APIRouter()

@router.post("/ingest", response_model=IngestResponse)
async def ingest(request: Request, body: IngestRequest) -> IngestResponse:
    '''
    Accepts raw JSON records and forwards them into the ingestion pipeline.
    '''

    ingestion_queue = request.app.state.ingestion_queue
    records = body.records()

    try:
        await ingestion_queue.enqueue(records)
    except QueueFullError:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="ingestion queue is full",
        )

    return IngestResponse(
        accepted_count=len(records),
        message=f'successfully ingested {len(records)} records.'
    )