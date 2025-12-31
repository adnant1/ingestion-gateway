from fastapi import APIRouter
from app.models import IngestRequest, IngestResponse

router = APIRouter()

@router.post("/ingest", response_model=IngestResponse)
async def ingest(request: IngestRequest) -> IngestResponse:
    '''
    Accepts raw JSON records and forwards them into the ingestion pipeline.
    '''

    records = request.records()

    # Placeholder for pipeline ingestion logic

    return IngestResponse(
        accepted_count=len(records),
        message=f'successfully ingested {len(records)} records.'
    )