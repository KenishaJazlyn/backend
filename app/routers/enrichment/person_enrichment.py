import time
from fastapi import APIRouter, HTTPException
from app.models.request.person_enrichment import EnrichName, EnrichConfirm, EnrichNamesList
from app.services.enrichment.person_enrichment_service import enrich_person_by_name, preview_person_enrichment
from app.db.neo4j_repo import get_repo

router = APIRouter()

@router.get("/health")
def health():
    return {"status":"ok"}

@router.post("")
def enrich_person(payload: EnrichName):
    """Auto enrich & save (use first QID found)"""
    res = enrich_person_by_name(payload.name)
    if res.get("status") == "not_found":
        raise HTTPException(status_code=404, detail=f"Person {payload.name} not found in internal DB")
    if res.get("status") == "qid_not_found":
        raise HTTPException(status_code=404, detail=f"Wikidata QID not found for {payload.name}")
    return res

@router.post("/batch")
def enrich_batch(offset: int = 0, limit: int = 100):
    repo = get_repo()
    with repo.driver.session(database=repo.db) as session:
        res = session.run("""
            MATCH (p:Person)
            WHERE NOT (p)-[:MEMBER_OF_DYNASTY]->(:Dynasty)
            RETURN p.name AS name, p.article_id AS article_id, p.full_name AS full_name
            SKIP $offset LIMIT $limit
        """, {"offset": offset, "limit": limit})
        persons = [dict(r) for r in res]
    results = []
    for p in persons:
        name = p.get('full_name')
        if not name:
            continue
        try:
            r = enrich_person_by_name(name)
            results.append({name: r})
        except Exception as e:
            results.append({name: {"status":"error", "error": str(e)}})
        time.sleep(0.5)
    return {"done": len(results), "results": results}



@router.post("/review")
def preview_person(payload: EnrichName):
    """Preview enrichment WITHOUT saving to DB"""
    res = preview_person_enrichment(payload.name)
    if res.get("status") == "not_found":
        raise HTTPException(status_code=404, detail=f"Person {payload.name} not found in internal DB")
    if res.get("status") == "qid_not_found":
        raise HTTPException(status_code=404, detail=f"Wikidata QID not found for {payload.name}")
    return res

@router.post("/confirm")
def confirm_person(payload: EnrichConfirm):
    """Save enrichment with specific QID after preview"""
    res = enrich_person_by_name(payload.name)
    if res.get("status") == "not_found":
        raise HTTPException(status_code=404, detail=f"Person {payload.name} not found in internal DB")
    return res


@router.post("/all-from-db")
def enrich_all_persons_from_db(offset: int = 100, limit: int = 200):
    """
    Enrich ALL persons dari Neo4j berdasarkan full_name.
    Tidak peduli sudah punya Dynasty atau belum - enrich semua!
    """
    repo = get_repo()
    
    # Ambil semua Person dari Neo4j
    with repo.driver.session(database=repo.db) as session:
        res = session.run("""
            MATCH (p:Person)
            WHERE p.full_name IS NOT NULL
            RETURN p.full_name AS full_name, p.article_id AS article_id
            SKIP $offset LIMIT $limit
        """, {"offset": offset, "limit": limit})
        persons = [dict(r) for r in res]
    
    results = []
    for p in persons:
        name = p.get('full_name')
        if not name:
            continue
        
        try:
            r = enrich_person_by_name(name)
            results.append({
                "name": name,
                "article_id": p.get('article_id'),
                "status": r.get("status"),
                "qid": r.get("qid")
            })
        except Exception as e:
            results.append({
                "name": name,
                "article_id": p.get('article_id'),
                "status": "error",
                "error": str(e)
            })
        
        time.sleep(0.5)  # Rate limit
    
    success_count = sum(1 for r in results if r.get("status") == "ok")
    
    return {
        "total": len(persons),
        "success": success_count,
        "failed": len(persons) - success_count,
        "results": results
    }

@router.post("/all-from-db-auto")
def enrich_all_auto():
    repo = get_repo()
    
    # Count total persons
    with repo.driver.session(database=repo.db) as session:
        count_res = session.run("""
            MATCH (p:Person)
            WHERE p.full_name IS NOT NULL
            RETURN count(p) AS total
        """)
        total = count_res.single()["total"]
    
    batch_size = 50
    offset = 0
    all_results = []
    
    while offset < total:
        with repo.driver.session(database=repo.db) as session:
            res = session.run("""
                MATCH (p:Person)
                WHERE p.full_name IS NOT NULL
                RETURN p.full_name AS full_name, p.article_id AS article_id
                SKIP $offset LIMIT $limit
            """, {"offset": offset, "limit": batch_size})
            persons = [dict(r) for r in res]
        
        batch_results = []
        for p in persons:
            name = p.get('full_name')
            if not name:
                continue
            
            try:
                r = enrich_person_by_name(name)
                batch_results.append({
                    "name": name,
                    "status": r.get("status"),
                    "qid": r.get("qid")
                })
            except Exception as e:
                batch_results.append({
                    "name": name,
                    "status": "error",
                    "error": str(e)
                })
            
            time.sleep(0.5)
        
        all_results.extend(batch_results)
        offset += batch_size
        
        print(f"✅ Progress: {offset}/{total} persons processed")
    
    success_count = sum(1 for r in all_results if r.get("status") == "ok")
    
    return {
        "total": total,
        "processed": len(all_results),
        "success": success_count,
        "failed": len(all_results) - success_count,
        "results": all_results
    }