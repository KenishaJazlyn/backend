from fastapi import APIRouter, HTTPException
from app.db.neo4j_repo import get_repo
import re
from datetime import datetime, date, time
from neo4j.time import Date, Time, DateTime, Duration

router = APIRouter()

FORBIDDEN = [
    "CREATE", "MERGE", "DELETE", "SET", "REMOVE", "DROP", "CALL", "LOAD CSV", "UNWIND"
]

def serialize_neo4j_types(obj):
    """
    Convert Neo4j types to JSON-serializable Python types.
    """
    if isinstance(obj, dict):
        return {k: serialize_neo4j_types(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [serialize_neo4j_types(item) for item in obj]
    elif isinstance(obj, (DateTime, datetime)):
        return obj.isoformat() if hasattr(obj, 'isoformat') else str(obj)
    elif isinstance(obj, (Date, date)):
        return obj.isoformat() if hasattr(obj, 'isoformat') else str(obj)
    elif isinstance(obj, (Time, time)):
        return obj.isoformat() if hasattr(obj, 'isoformat') else str(obj)
    elif isinstance(obj, Duration):
        return str(obj)
    elif obj is None:
        return None
    else:
        return obj

@router.get("/infobox/{element_id}")
def infobox_id(element_id: str):
    """
    Kembalikan properties dari node dengan element id tertentu.
    Element ID format: <database_id>:<uuid>:<sequence>
    """
    # Validate element_id is not empty
    if not element_id or not element_id.strip():
        raise HTTPException(status_code=400, detail="Invalid element_id: cannot be empty.")
    
    repo = get_repo()
    try:
        with repo.driver.session(database=repo.db) as session:
            result = session.run("MATCH (n) WHERE elementId(n) = $element_id RETURN n", element_id=element_id)
            record = result.single()
            if not record:
                raise HTTPException(status_code=404, detail=f"Node with element_id {element_id} not found")
            node = record["n"]
            properties = serialize_neo4j_types(dict(node))
            return {"status": "ok", "element_id": element_id, "labels": list(node.labels), "properties": properties}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cypher error: {e}")