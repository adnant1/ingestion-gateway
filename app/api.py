from fastapi import APIRouter
from app.models import IngestRequest, IngestResponse

router = APIRouter()

@router.post("/ingest")
async def ingest(request: IngestRequest) -> IngestResponse:
    '''
    Ingest data into the specified storage backend.

    :param request: IngestRequest object containing raw JSON data.
    :return: IngestResponse object indicating success or failure.
    '''

    return