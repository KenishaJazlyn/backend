import time
from fastapi import APIRouter, HTTPException
from app.models.person_enrichment import EnrichName, EnrichConfirm, EnrichNamesList
from app.services.event_enrichment import  enrich_all_events
from app.db.neo4j_repo import get_repo

router = APIRouter()

@router.post("")
def enrich_event():
    results = enrich_all_events()
    return {"done": len(results), "results": results}