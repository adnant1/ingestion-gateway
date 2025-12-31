from fastapi import FastAPI
from app.api import router

app = FastAPI(
    title="multi-storage ingestion gateway",
    description="async ingestion gateway for multiple storage backends",
)

app.include_router(router)