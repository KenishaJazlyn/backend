from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from app.db.neo4j_repo import get_repo
from app.routers.enrichment.person_enrichment import router as person_enrichment_router
from app.routers.enrichment.event_enrichment import router as event_enrichment_router
from app.routers.health import router as health_router
from app.routers.feature.explore_cypher import router as explore_router
from app.routers.feature.infobox import router as infobox_router
from app.routers.enrichment.country_enrichment import router as country_enrichment_router

from app.routers.feature.searching import router as searching_router
app = FastAPI(title="KG Enrichment Service - Person")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(person_enrichment_router, prefix="/enrich/persons")
app.include_router(event_enrichment_router, prefix="/enrich/events")
app.include_router(country_enrichment_router, prefix="/enrich/countries")
app.include_router(explore_router)
app.include_router(infobox_router)
app.include_router(searching_router)