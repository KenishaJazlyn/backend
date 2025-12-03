import time
import asyncio
import aiohttp
import json
import os
from concurrent.futures import ThreadPoolExecutor
from fastapi import APIRouter, HTTPException, BackgroundTasks
from app.models.request.person_enrichment import EnrichName, EnrichConfirm, EnrichNamesList
from app.services.enrichment.person_enrichment_service import enrich_person_by_name, preview_person_enrichment
from app.db.neo4j_repo import get_repo

router = APIRouter()

# File untuk simpan progress (biar bisa resume kalau mati)
PROGRESS_FILE = "enrichment_progress.json"

# Global progress tracking
enrichment_progress = {
    "running": False,
    "total": 0,
    "processed": 0,
    "success": 0,
    "failed": 0,
    "current_batch": 0,
    "last_offset": 0,
    # Detailed failure tracking
    "fail_reasons": {
        "not_found": 0,       # Person not in Neo4j
        "qid_not_found": 0,   # No Wikidata QID match
        "error": 0,           # Exception/timeout
        "skipped": 0          # No name
    },
    "last_errors": []         # Last 10 error messages
}

def save_progress():
    """Save progress ke file"""
    try:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(enrichment_progress, f)
    except Exception as e:
        print(f"⚠️ Failed to save progress: {e}")

def load_progress() -> dict:
    """Load progress dari file"""
    try:
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"⚠️ Failed to load progress: {e}")
    return None

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


# ============== FAST ENRICHMENT (Parallel) ==============

def enrich_single_person(person: dict) -> dict:
    """Enrich single person - untuk parallel processing"""
    name = person.get('full_name')
    article_id = person.get('article_id')
    
    if not name:
        return {"name": name, "article_id": article_id, "status": "skipped", "reason": "no name"}
    
    try:
        r = enrich_person_by_name(name)
        status = r.get("status")
        return {
            "name": name,
            "article_id": article_id,
            "status": status,
            "qid": r.get("qid"),
            "message": r.get("message")  # Include any error message
        }
    except Exception as e:
        return {
            "name": name,
            "article_id": article_id,
            "status": "error",
            "error": str(e)
        }


@router.post("/fast-enrich")
def fast_enrich_batch(
    offset: int = 0, 
    limit: int = 100,
    workers: int = 5,  # Parallel workers (jangan terlalu banyak, nanti kena rate limit Wikidata)
    delay: float = 0.2  # Delay antar request (detik)
):
    """
    Fast parallel enrichment dengan ThreadPoolExecutor.
    - workers: jumlah parallel threads (default 5, max recommended 10)
    - delay: delay antar batch untuk avoid rate limiting
    """
    repo = get_repo()
    
    with repo.driver.session(database=repo.db) as session:
        res = session.run("""
            MATCH (p:Person)
            WHERE p.full_name IS NOT NULL
            RETURN p.full_name AS full_name, p.article_id AS article_id
            SKIP $offset LIMIT $limit
        """, {"offset": offset, "limit": limit})
        persons = [dict(r) for r in res]
    
    if not persons:
        return {"message": "No more persons to process", "offset": offset}
    
    results = []
    
    # Process in parallel with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []
        for i, person in enumerate(persons):
            future = executor.submit(enrich_single_person, person)
            futures.append(future)
            
            # Small delay between submissions to avoid overwhelming Wikidata
            if i % workers == 0 and i > 0:
                time.sleep(delay)
        
        # Collect results
        for future in futures:
            try:
                result = future.result(timeout=30)
                results.append(result)
            except Exception as e:
                results.append({"status": "error", "error": str(e)})
    
    success_count = sum(1 for r in results if r.get("status") == "ok")
    
    return {
        "offset": offset,
        "limit": limit,
        "processed": len(results),
        "success": success_count,
        "failed": len(results) - success_count,
        "next_offset": offset + limit,
        "results": results
    }


def fetch_persons_batch(repo, offset: int, batch_size: int) -> list:
    """Fetch batch of persons dengan fresh connection - SKIP yang sudah punya QID"""
    try:
        with repo.driver.session(database=repo.db) as session:
            res = session.run("""
                MATCH (p:Person)
                WHERE p.full_name IS NOT NULL
                AND p.wikidata_qid IS NULL
                RETURN p.full_name AS full_name, p.article_id AS article_id
                SKIP $offset LIMIT $limit
            """, {"offset": offset, "limit": batch_size})
            return [dict(r) for r in res]
    except Exception as e:
        print(f"⚠️ Error fetching batch at offset {offset}: {e}")
        return None  # Return None to indicate retry needed


def background_enrich_all(batch_size: int = 100, workers: int = 5, delay: float = 0.3, start_offset: int = 0):
    """Background task untuk enrich semua data"""
    global enrichment_progress
    
    repo = get_repo()
    
    # Count total dengan fresh connection - HANYA yang belum punya QID
    try:
        with repo.driver.session(database=repo.db) as session:
            count_res = session.run("""
                MATCH (p:Person)
                WHERE p.full_name IS NOT NULL
                AND p.wikidata_qid IS NULL
                RETURN count(p) AS total
            """)
            total = count_res.single()["total"]
    except Exception as e:
        print(f"❌ Failed to count persons: {e}")
        enrichment_progress["running"] = False
        return
    
    enrichment_progress["total"] = total
    enrichment_progress["running"] = True
    
    # Kalau resume, keep existing counts
    if start_offset == 0:
        enrichment_progress["processed"] = 0
        enrichment_progress["success"] = 0
        enrichment_progress["failed"] = 0
    
    offset = start_offset
    enrichment_progress["last_offset"] = offset
    
    retry_count = 0
    max_retries = 3
    
    while offset < total and enrichment_progress["running"]:
        enrichment_progress["current_batch"] = offset
        enrichment_progress["last_offset"] = offset
        save_progress()  # Save setiap batch
        
        # Fetch dengan retry mechanism
        persons = fetch_persons_batch(repo, offset, batch_size)
        
        if persons is None:
            # Connection error, retry
            retry_count += 1
            if retry_count >= max_retries:
                print(f"❌ Max retries reached at offset {offset}. Stopping.")
                break
            print(f"🔄 Retry {retry_count}/{max_retries} after connection error...")
            time.sleep(2)  # Wait before retry
            continue
        
        retry_count = 0  # Reset retry count on success
        
        if not persons:
            break
        
        # Parallel processing
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(enrich_single_person, p) for p in persons]
            
            for future in futures:
                try:
                    result = future.result(timeout=30)
                    enrichment_progress["processed"] += 1
                    status = result.get("status")
                    
                    if status == "ok":
                        enrichment_progress["success"] += 1
                    else:
                        enrichment_progress["failed"] += 1
                        # Track failure reason
                        if status in enrichment_progress["fail_reasons"]:
                            enrichment_progress["fail_reasons"][status] += 1
                        else:
                            enrichment_progress["fail_reasons"]["error"] += 1
                        
                        # Keep last 10 errors for debugging
                        error_info = {"name": result.get("name"), "status": status, "message": result.get("message") or result.get("error")}
                        enrichment_progress["last_errors"].append(error_info)
                        if len(enrichment_progress["last_errors"]) > 10:
                            enrichment_progress["last_errors"].pop(0)
                            
                except Exception as e:
                    print(f"⚠️ Future error: {e}")
                    enrichment_progress["processed"] += 1
                    enrichment_progress["failed"] += 1
                    enrichment_progress["fail_reasons"]["error"] += 1
        
        offset += batch_size
        enrichment_progress["last_offset"] = offset
        save_progress()  # Save after each batch
        
        time.sleep(delay)  # Rate limit between batches
        
        print(f"✅ Progress: {enrichment_progress['processed']}/{total} (offset: {offset})")
    
    enrichment_progress["running"] = False
    save_progress()
    print(f"🏁 Enrichment finished! Total: {enrichment_progress['processed']}, Success: {enrichment_progress['success']}, Failed: {enrichment_progress['failed']}")


@router.post("/start-fast-enrich-all")
def start_fast_enrich_all(
    background_tasks: BackgroundTasks,
    batch_size: int = 100,
    workers: int = 5,
    delay: float = 0.3
):
    """
    Start background enrichment untuk SEMUA persons.
    - batch_size: berapa data per batch (default 100)
    - workers: parallel threads (default 5)
    - delay: delay antar batch dalam detik (default 0.3)
    
    Untuk 11,000 data dengan setting default:
    - Estimasi waktu: ~30-60 menit (tergantung Wikidata response)
    """
    global enrichment_progress
    
    if enrichment_progress["running"]:
        return {"status": "already_running", "progress": enrichment_progress}
    
    background_tasks.add_task(background_enrich_all, batch_size, workers, delay, 0)
    
    return {
        "status": "started",
        "message": "Background enrichment started. Check /progress for status.",
        "settings": {
            "batch_size": batch_size,
            "workers": workers,
            "delay": delay
        }
    }


@router.post("/resume-enrich")
def resume_enrichment(
    background_tasks: BackgroundTasks,
    batch_size: int = 100,
    workers: int = 5,
    delay: float = 0.3
):
    """
    Resume enrichment dari posisi terakhir (kalau mati/restart).
    Otomatis baca last_offset dari file progress.
    """
    global enrichment_progress
    
    if enrichment_progress["running"]:
        return {"status": "already_running", "progress": enrichment_progress}
    
    # Load saved progress
    saved = load_progress()
    if saved:
        enrichment_progress.update(saved)
        start_offset = saved.get("last_offset", 0)
        print(f"📂 Resuming from offset {start_offset}")
    else:
        start_offset = 0
        print("📂 No saved progress found, starting from 0")
    
    background_tasks.add_task(background_enrich_all, batch_size, workers, delay, start_offset)
    
    return {
        "status": "resumed",
        "message": f"Enrichment resumed from offset {start_offset}",
        "previous_progress": {
            "processed": enrichment_progress.get("processed", 0),
            "success": enrichment_progress.get("success", 0),
            "failed": enrichment_progress.get("failed", 0)
        },
        "settings": {
            "batch_size": batch_size,
            "workers": workers,
            "delay": delay,
            "start_offset": start_offset
        }
    }


@router.post("/reset-progress")
def reset_progress():
    """Reset progress dan hapus file simpanan"""
    global enrichment_progress
    
    if enrichment_progress["running"]:
        return {"status": "error", "message": "Cannot reset while running. Stop first."}
    
    enrichment_progress = {
        "running": False,
        "total": 0,
        "processed": 0,
        "success": 0,
        "failed": 0,
        "current_batch": 0,
        "last_offset": 0,
        "fail_reasons": {
            "not_found": 0,
            "qid_not_found": 0,
            "error": 0,
            "skipped": 0
        },
        "last_errors": []
    }
    
    try:
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
    except:
        pass
    
    return {"status": "ok", "message": "Progress reset to 0"}


@router.get("/progress")
def get_enrichment_progress():
    """Check progress of background enrichment - INSTANT, no DB query"""
    global enrichment_progress
    
    # Also try to load from file if not running (in case server restarted)
    if not enrichment_progress["running"] and enrichment_progress["processed"] == 0:
        saved = load_progress()
        if saved:
            enrichment_progress.update(saved)
    
    progress_pct = 0
    if enrichment_progress["total"] > 0:
        progress_pct = round(enrichment_progress["processed"] / enrichment_progress["total"] * 100, 2)
    
    return {
        **enrichment_progress,
        "progress_percent": progress_pct
    }


@router.post("/stop-enrich")
def stop_enrichment():
    """Stop background enrichment"""
    global enrichment_progress
    enrichment_progress["running"] = False
    return {"status": "stopping", "message": "Enrichment will stop after current batch"}