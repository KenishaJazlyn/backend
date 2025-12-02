from fastapi import APIRouter, HTTPException
from app.db.neo4j_repo import get_repo
import re
from datetime import datetime, date, time
from neo4j.time import Date, Time, DateTime, Duration

router = APIRouter()

FORBIDDEN = [
    "CREATE", "MERGE", "DELETE", "SET", "REMOVE", "DROP", "CALL", "LOAD CSV", "UNWIND"
]

EXCLUDED_PROPERTIES = {"embedding", "embedding_updated", "searchable_text"}


def filter_properties(obj):
    """
    Remove excluded properties from the object.
    """
    if isinstance(obj, dict):
        return {k: filter_properties(v) for k, v in obj.items() if k not in EXCLUDED_PROPERTIES}
    elif isinstance(obj, (list, tuple)):
        return [filter_properties(item) for item in obj]
    else:
        return obj

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


def get_related_nodes(session, element_id):
    """
    Fetch related nodes (max 5 Person, max 5 Event, max 5 others) connected to the given node.
    Returns a list of dicts with element_id, relationship, labels, and properties.
    """
    related_result = session.run(
        """
        MATCH (n)-[r]-(m) 
        WHERE elementId(n) = $element_id
        WITH m, type(r) AS relationship, labels(m) AS labels
        ORDER BY labels(m), m.name
        WITH 
          collect(CASE WHEN 'Person' IN labels THEN {element_id: elementId(m), relationship: relationship, labels: labels, node: m} END) AS persons,
          collect(CASE WHEN 'Event' IN labels THEN {element_id: elementId(m), relationship: relationship, labels: labels, node: m} END) AS events,
          collect(CASE WHEN NOT ('Person' IN labels OR 'Event' IN labels) THEN {element_id: elementId(m), relationship: relationship, labels: labels, node: m} END) AS others
        RETURN persons[0..5] + events[0..5] + others[0..5] AS all_related
        """,
        element_id=element_id
    )
    
    related_nodes = []
    for rel_record in related_result:
        for item in rel_record["all_related"]:
            if item is not None:
                related_nodes.append({
                    "element_id": item["element_id"],
                    "relationship": item["relationship"],
                    "labels": item["labels"],
                    "properties": serialize_neo4j_types(dict(item["node"]))
                })
    
    return related_nodes

@router.get("/infobox/{element_id}")
def infobox_id(element_id: str):
    """
    Kembalikan properties dari node dengan element id tertentu, beserta related nodes.
    Element ID format: <database_id>:<uuid>:<sequence>
    """
    # Validate element_id is not empty
    if not element_id or not element_id.strip():
        raise HTTPException(status_code=400, detail="Invalid element_id: cannot be empty.")
    
    repo = get_repo()
    try:
        with repo.driver.session(database=repo.db) as session:
            # Get the main node
            result = session.run("MATCH (n) WHERE elementId(n) = $element_id RETURN n", element_id=element_id)
            record = result.single()
            if not record:
                raise HTTPException(status_code=404, detail=f"Node with element_id {element_id} not found")
            node = record["n"]
            properties = serialize_neo4j_types(filter_properties(dict(node)))
            
            # Get related nodes
            related_nodes = get_related_nodes(session, element_id)
            
            return {
                "status": "ok",
                "element_id": element_id,
                "labels": list(node.labels),
                "properties": properties,
                "related_nodes": related_nodes
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cypher error: {e}")