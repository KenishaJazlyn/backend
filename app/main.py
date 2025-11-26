from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from app.db.neo4j_repo import get_repo
from app.routers.enrichment.person_enrichment import router as person_enrichment_router
from app.routers.enrichment.event_enrichment import router as event_enrichment_router
from app.routers.health import router as health_router
from app.routers.feature.explore_cypher import router as explore_router
app = FastAPI(title="KG Enrichment Service - Person")

app.include_router(health_router)
app.include_router(person_enrichment_router, prefix="/enrich/persons")
app.include_router(event_enrichment_router, prefix="/enrich/events")
app.include_router(explore_router)
