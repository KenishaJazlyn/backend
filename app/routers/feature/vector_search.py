from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List
import time

from app.db.vector_repo import get_vector_repo, reset_vector_dimension
from app.services.feature.vector_service import (
    generate_embedding,
    generate_embeddings_batch,
    create_searchable_text_person,
    create_searchable_text_event,
    compute_similarity,
    get_embedding_dimension,
    reset_model,
    DEFAULT_MODEL
)

router = APIRouter()


class SemanticSearchRequest(BaseModel):
    query: str
    limit: Optional[int] = 20
    min_score: Optional[float] = 0.3
    search_type: Optional[str] = "all"  # "person", "event", "all"


class HybridSearchRequest(BaseModel):
    query: str
    limit: Optional[int] = 20
    keyword_weight: Optional[float] = 0.4
    semantic_weight: Optional[float] = 0.6
    search_type: Optional[str] = "all"


@router.post("/setup-indexes")
def setup_vector_indexes():
    """Setup vector indexes di Neo4j (jalankan sekali setelah generate embeddings)"""
    try:
        repo = get_vector_repo()
        result = repo.create_vector_index()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create indexes: {str(e)}")


@router.get("/model-info")
def get_model_info():
    """Get info about current embedding model"""
    try:
        dimension = get_embedding_dimension()
        return {
            "default_model": DEFAULT_MODEL,
            "dimension": dimension,
            "note": "Set EMBEDDING_MODEL env var to override default model"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/clear-all-embeddings")
def clear_all_embeddings():
    """
    Clear SEMUA embeddings untuk regenerate dengan model/text baru.
    ⚠️ HATI-HATI: Ini akan hapus semua embeddings!
    """
    try:
        repo = get_vector_repo()
        
        with repo.driver.session(database=repo.db) as session:
            # Clear person embeddings
            result_person = session.run("""
                MATCH (p:Person)
                WHERE p.embedding IS NOT NULL
                SET p.embedding = null, p.searchable_text = null, p.embedding_failed = null
                RETURN count(p) as cleared
            """)
            persons_cleared = result_person.single()["cleared"]
            
            # Clear event embeddings  
            result_event = session.run("""
                MATCH (e:Event)
                WHERE e.embedding IS NOT NULL
                SET e.embedding = null, e.searchable_text = null, e.embedding_failed = null
                RETURN count(e) as cleared
            """)
            events_cleared = result_event.single()["cleared"]
            
            # Also clear failed flags
            session.run("""
                MATCH (p:Person)
                WHERE p.embedding_failed IS NOT NULL
                SET p.embedding_failed = null
            """)
            session.run("""
                MATCH (e:Event)
                WHERE e.embedding_failed IS NOT NULL
                SET e.embedding_failed = null
            """)
        
        # Reset model and dimension cache
        reset_model()
        reset_vector_dimension()
        
        return {
            "status": "ok",
            "message": "All embeddings cleared",
            "persons_cleared": persons_cleared,
            "events_cleared": events_cleared,
            "note": "Now run /setup-indexes then /generate-embeddings/persons and /events"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/full-reset-and-regenerate")
def full_reset_and_regenerate(batch_size: int = 50):
    """
    Full reset: Clear embeddings, recreate indexes, regenerate all.
    ⚠️ INI AKAN LAMA! Gunakan untuk perubahan model/text besar.
    """
    try:
        # 1. Clear embeddings
        clear_result = clear_all_embeddings()
        
        # 2. Setup indexes dengan dimension baru
        repo = get_vector_repo()
        index_result = repo.create_vector_index()
        
        return {
            "status": "reset_complete",
            "cleared": clear_result,
            "indexes": index_result,
            "next_step": "Now call /generate-embeddings/persons and /generate-embeddings/events"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/check-indexes")
def check_vector_indexes():
    """Check apakah vector indexes sudah ada dan ready"""
    try:
        repo = get_vector_repo()
        return repo.check_vector_index_exists()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/generate-embeddings/persons")
def generate_person_embeddings(batch_size: int = 50):
    """Generate embeddings untuk semua Person yang belum punya"""
    repo = get_vector_repo()
    
    total_processed = 0
    total_success = 0
    total_failed = 0
    
    while True:
        persons = repo.get_persons_without_embedding(limit=batch_size)
        
        if not persons:
            break
        
        searchable_texts = [create_searchable_text_person(p) for p in persons]
        
        try:
            embeddings = generate_embeddings_batch(searchable_texts)
            
            for i, person in enumerate(persons):
                article_id = person.get("article_id")
                
                if not article_id or not searchable_texts[i].strip():
                    total_failed += 1
                    continue
                
                if embeddings[i] and len(embeddings[i]) > 0:
                    repo.store_person_embedding(article_id, embeddings[i], searchable_texts[i])
                    total_success += 1
                else:
                    repo.mark_embedding_failed(article_id, "Empty embedding")
                    total_failed += 1
                
                total_processed += 1
            
            print(f"✅ Processed {total_processed} persons, {total_success} success")
            
        except Exception as e:
            print(f"❌ Batch error: {e}")
            break
        
        time.sleep(0.1)
    
    return {"total_processed": total_processed, "total_success": total_success, "total_failed": total_failed}


@router.post("/generate-embeddings/events")
def generate_event_embeddings(batch_size: int = 50):
    """Generate embeddings untuk semua Event yang belum punya"""
    repo = get_vector_repo()
    
    total_processed = 0
    total_success = 0
    total_failed = 0
    
    while True:
        events = repo.get_events_without_embedding(limit=batch_size)
        
        if not events:
            break
        
        searchable_texts = [create_searchable_text_event(e) for e in events]
        
        try:
            embeddings = generate_embeddings_batch(searchable_texts)
            
            for i, event in enumerate(events):
                event_id = event.get("event_id")
                
                if not event_id or not searchable_texts[i].strip():
                    total_failed += 1
                    continue
                
                if embeddings[i] and len(embeddings[i]) > 0:
                    repo.store_event_embedding(event_id, embeddings[i], searchable_texts[i])
                    total_success += 1
                else:
                    repo.mark_event_embedding_failed(event_id, "Empty embedding")
                    total_failed += 1
                
                total_processed += 1
            
            print(f"✅ Processed {total_processed} events, {total_success} success")
            
        except Exception as e:
            print(f"❌ Batch error: {e}")
            break
        
        time.sleep(0.1)
    
    return {"total_processed": total_processed, "total_success": total_success, "total_failed": total_failed}


@router.get("/embedding-stats")
def get_embedding_statistics():
    """Get statistics tentang embeddings"""
    try:
        repo = get_vector_repo()
        stats = repo.get_embedding_stats()
        
        return {
            "persons": {
                "total": stats["total_persons"],
                "with_embedding": stats["persons_with_embedding"],
                "percentage": round(stats["persons_with_embedding"] / max(stats["total_persons"], 1) * 100, 2)
            },
            "events": {
                "total": stats["total_events"],
                "with_embedding": stats["events_with_embedding"],
                "percentage": round(stats["events_with_embedding"] / max(stats["total_events"], 1) * 100, 2)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/semantic-search")
def semantic_search(payload: SemanticSearchRequest):
    """
    🚀 Semantic search menggunakan NEO4J NATIVE VECTOR INDEX.
    Jauh lebih cepat daripada manual calculation!
    """
    if len(payload.query.strip()) < 2:
        raise HTTPException(status_code=400, detail="Query minimal 2 karakter")
    
    repo = get_vector_repo()
    query_text = payload.query.strip()
    
    # Generate embedding untuk query
    query_embedding = generate_embedding(query_text)
    
    if not query_embedding:
        raise HTTPException(status_code=500, detail="Failed to generate query embedding")
    
    results = {
        "query": query_text,
        "search_type": "semantic_native_vector",
        "persons": [],
        "events": []
    }
    
    try:
        # Search Persons using NATIVE VECTOR INDEX
        if payload.search_type in ["person", "all"]:
            persons = repo.vector_search_persons(
                query_embedding=query_embedding,
                limit=payload.limit,
                min_score=payload.min_score
            )
            
            for p in persons:
                results["persons"].append({
                    "type": "person",
                    "element_id": p["element_id"],
                    "name": p["name"],
                    "description": p["description"],
                    "image": p["image"],
                    "similarity_score": round(p["similarity_score"], 4),
                    "context": {
                        "positions": p.get("positions", []),
                        "country": p.get("country"),
                        "birth_date": p.get("birth_date"),
                        "death_date": p.get("death_date")
                    }
                })
        
        # Search Events using NATIVE VECTOR INDEX
        if payload.search_type in ["event", "all"]:
            events = repo.vector_search_events(
                query_embedding=query_embedding,
                limit=payload.limit,
                min_score=payload.min_score
            )
            
            for e in events:
                results["events"].append({
                    "type": "event",
                    "element_id": e["element_id"],
                    "name": e["name"],
                    "description": e["description"],
                    "image": e["image"],
                    "similarity_score": round(e["similarity_score"], 4),
                    "context": {
                        "country": e.get("country"),
                        "impact": e.get("impact"),
                        "start_date": e.get("start_date"),
                        "end_date": e.get("end_date")
                    }
                })
        
        return results
        
    except Exception as e:
        error_msg = str(e)
        if "person_embedding_index" in error_msg or "event_embedding_index" in error_msg:
            raise HTTPException(
                status_code=400, 
                detail="Vector index belum dibuat. Jalankan POST /vector/setup-indexes dulu!"
            )
        raise HTTPException(status_code=500, detail=f"Search error: {error_msg}")


@router.post("/hybrid-search")
def hybrid_search(payload: HybridSearchRequest):
    """
    Hybrid search: Neo4j Native Vector + Keyword Boosting.
    Best of both worlds!
    """
    if len(payload.query.strip()) < 2:
        raise HTTPException(status_code=400, detail="Query minimal 2 karakter")
    
    repo = get_vector_repo()
    query_text = payload.query.strip()
    query_lower = query_text.lower()
    
    query_embedding = generate_embedding(query_text)
    
    if not query_embedding:
        raise HTTPException(status_code=500, detail="Failed to generate query embedding")
    
    results = {
        "query": query_text,
        "search_type": "hybrid",
        "weights": {"keyword": payload.keyword_weight, "semantic": payload.semantic_weight},
        "persons": [],
        "events": []
    }
    
    try:
        # Get semantic results from Native Vector Index
        if payload.search_type in ["person", "all"]:
            persons = repo.vector_search_persons(
                query_embedding=query_embedding,
                limit=payload.limit * 2,  # Get more for re-ranking
                min_score=0.2  # Lower threshold, will filter after
            )
            
            # Re-rank with keyword boost
            scored_persons = []
            for p in persons:
                name = p["name"] or ""
                name_lower = name.lower()
                desc_lower = (p["description"] or "").lower()
                
                # Keyword score
                keyword_score = 0.0
                if name_lower == query_lower:
                    keyword_score = 1.0
                elif query_lower in name_lower:
                    keyword_score = 0.8
                elif any(word in name_lower for word in query_lower.split() if len(word) > 2):
                    keyword_score = 0.5
                elif query_lower in desc_lower:
                    keyword_score = 0.3
                
                # Hybrid score
                semantic_score = p["similarity_score"]
                hybrid_score = (
                    payload.keyword_weight * keyword_score +
                    payload.semantic_weight * semantic_score
                )
                
                scored_persons.append({
                    **p,
                    "keyword_score": keyword_score,
                    "semantic_score": semantic_score,
                    "hybrid_score": hybrid_score
                })
            
            # Sort by hybrid score and take top N
            scored_persons.sort(key=lambda x: x["hybrid_score"], reverse=True)
            
            for p in scored_persons[:payload.limit]:
                results["persons"].append({
                    "type": "person",
                    "element_id": p["element_id"],
                    "name": p["name"],
                    "description": p["description"],
                    "image": p["image"],
                    "scores": {
                        "keyword": round(p["keyword_score"], 4),
                        "semantic": round(p["semantic_score"], 4),
                        "hybrid": round(p["hybrid_score"], 4)
                    },
                    "context": {
                        "positions": p.get("positions", []),
                        "country": p.get("country")
                    }
                })
        
        # Events hybrid search
        if payload.search_type in ["event", "all"]:
            events = repo.vector_search_events(
                query_embedding=query_embedding,
                limit=payload.limit * 2,
                min_score=0.2
            )
            
            scored_events = []
            for e in events:
                name = e["name"] or ""
                name_lower = name.lower()
                desc_lower = (e["description"] or "").lower()
                
                keyword_score = 0.0
                if name_lower == query_lower:
                    keyword_score = 1.0
                elif query_lower in name_lower:
                    keyword_score = 0.8
                elif any(word in name_lower for word in query_lower.split() if len(word) > 2):
                    keyword_score = 0.5
                elif query_lower in desc_lower:
                    keyword_score = 0.3
                
                semantic_score = e["similarity_score"]
                hybrid_score = (
                    payload.keyword_weight * keyword_score +
                    payload.semantic_weight * semantic_score
                )
                
                scored_events.append({
                    **e,
                    "keyword_score": keyword_score,
                    "semantic_score": semantic_score,
                    "hybrid_score": hybrid_score
                })
            
            scored_events.sort(key=lambda x: x["hybrid_score"], reverse=True)
            
            for e in scored_events[:payload.limit]:
                results["events"].append({
                    "type": "event",
                    "element_id": e["element_id"],
                    "name": e["name"],
                    "description": e["description"],
                    "image": e["image"],
                    "scores": {
                        "keyword": round(e["keyword_score"], 4),
                        "semantic": round(e["semantic_score"], 4),
                        "hybrid": round(e["hybrid_score"], 4)
                    },
                    "context": {
                        "country": e.get("country"),
                        "impact": e.get("impact")
                    }
                })
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Hybrid search error: {str(e)}")


@router.get("/similar/person/{element_id}")
def find_similar_persons(element_id: str, limit: int = 10, min_score: float = 0.5):
    """Find similar persons using Native Vector Index"""
    try:
        repo = get_vector_repo()
        return repo.find_similar_persons(element_id, limit, min_score)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/similar/event/{element_id}")
def find_similar_events(element_id: str, limit: int = 10, min_score: float = 0.5):
    """Find similar events using Native Vector Index"""
    try:
        repo = get_vector_repo()
        return repo.find_similar_events(element_id, limit, min_score)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))