from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from app.services.enrich_service import enrich_person_by_name, preview_person_enrichment
from app.db.neo4j_repo import get_repo
from app.routers.person_enrichment import router as person_enrichment_router

app = FastAPI(title="KG Enrichment Service - Person")

app.include_router(person_enrichment_router, prefix="/enrich/persons")