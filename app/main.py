# main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from app.services.enrich_service import enrich_person_by_name
from app.db.neo4j_repo import get_repo

app = FastAPI(title="KG Enrichment Service - Person")

class EnrichName(BaseModel):
    name: str

@app.get("/health")
def health():
    return {"status":"ok"}

@app.post("/enrich/person")
def enrich_person(payload: EnrichName):
    res = enrich_person_by_name(payload.name)
    if res.get("status") == "not_found":
        raise HTTPException(status_code=404, detail=f"Person {payload.name} not found in internal DB")
    if res.get("status") == "qid_not_found":
        raise HTTPException(status_code=404, detail=f"Wikidata QID not found for {payload.name}")
    return res

@app.post("/enrich/persons")
def enrich_all():
    repo = get_repo()
    persons = repo.get_all_persons(limit=10000)
    results = []
    for p in persons:
        name = p.get('name')
        if not name:
            continue
        try:
            r = enrich_person_by_name(name)
            results.append({name: r})
        except Exception as e:
            results.append({name: {"status":"error", "error": str(e)}})
    return {"done": len(results), "results": results}
