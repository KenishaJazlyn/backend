from fastapi import APIRouter, HTTPException
from app.db.neo4j_repo import get_repo
from app.models.request.cypherRequest import CypherQueryRequest
import re

router = APIRouter()

FORBIDDEN = [
    "CREATE", "MERGE", "DELETE", "SET", "REMOVE", "DROP", "CALL", "LOAD CSV", "UNWIND"
]

def is_safe_cypher(query: str) -> bool:
    pattern = r"\b(" + "|".join(FORBIDDEN) + r")\b"
    return not re.search(pattern, query, re.IGNORECASE)

@router.post("/explore/cypher")
def run_cypher_query(payload: CypherQueryRequest):
    """
    Jalankan Cypher query custom dari user ke Neo4j.
    Hanya untuk eksplorasi data (tidak boleh mengubah DB).
    """
    if not is_safe_cypher(payload.query):
        raise HTTPException(
            status_code=403,
            detail="Forbidden Cypher command detected! Only read-only queries (MATCH/RETURN) are allowed."
        )
    repo = get_repo()
    try:
        with repo.driver.session(database=repo.db) as session:
            result = session.run(payload.query)
            rows = [dict(r) for r in result]
        return {"status": "ok", "results": rows}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cypher error: {e}")