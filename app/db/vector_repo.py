from neo4j import GraphDatabase
from typing import List, Optional
import os

# Dimension akan di-set dynamically dari model
# Default 768 untuk model baru (BGE, E5, dll)
# 384 untuk all-MiniLM-L6-v2
VECTOR_DIMENSION = None  # Will be set from model

def get_vector_dimension():
    """Get dimension from loaded model"""
    global VECTOR_DIMENSION
    if VECTOR_DIMENSION is None:
        # Import here to avoid circular import
        from app.services.feature.vector_service import get_embedding_dimension
        VECTOR_DIMENSION = get_embedding_dimension()
    return VECTOR_DIMENSION

def reset_vector_dimension():
    """Reset dimension (jika model diganti)"""
    global VECTOR_DIMENSION
    VECTOR_DIMENSION = None

class VectorRepository:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            auth=(
                os.getenv("NEO4J_USER", "neo4j"),
                os.getenv("NEO4J_PASSWORD", "password")
            )
        )
        self.db = os.getenv("NEO4J_DATABASE", "neo4j")
    
    def create_vector_index(self, dimension: int = None):
        """Create vector indexes untuk Person dan Event (jalankan sekali)"""
        dim = dimension or get_vector_dimension()
        print(f"📐 Creating vector indexes with dimension: {dim}")
        
        with self.driver.session(database=self.db) as session:
            # Drop existing indexes jika ada (untuk recreate)
            try:
                session.run("DROP INDEX person_embedding_index IF EXISTS")
                session.run("DROP INDEX event_embedding_index IF EXISTS")
                print("🗑️ Dropped existing indexes")
            except:
                pass
            
            # Create Person vector index
            session.run("""
                CREATE VECTOR INDEX person_embedding_index IF NOT EXISTS
                FOR (p:Person)
                ON (p.embedding)
                OPTIONS {
                    indexConfig: {
                        `vector.dimensions`: $dimensions,
                        `vector.similarity_function`: 'cosine'
                    }
                }
            """, {"dimensions": dim})
            
            # Create Event vector index
            session.run("""
                CREATE VECTOR INDEX event_embedding_index IF NOT EXISTS
                FOR (e:Event)
                ON (e.embedding)
                OPTIONS {
                    indexConfig: {
                        `vector.dimensions`: $dimensions,
                        `vector.similarity_function`: 'cosine'
                    }
                }
            """, {"dimensions": dim})
            
            return {"status": "ok", "message": f"Vector indexes created with dimension {dim}"}
    
    def check_vector_index_exists(self) -> dict:
        """Check apakah vector indexes sudah ada"""
        with self.driver.session(database=self.db) as session:
            result = session.run("""
                SHOW INDEXES
                WHERE type = 'VECTOR'
            """)
            indexes = [dict(r) for r in result]
            
            return {
                "person_index": any(idx.get("name") == "person_embedding_index" for idx in indexes),
                "event_index": any(idx.get("name") == "event_embedding_index" for idx in indexes),
                "indexes": indexes
            }
    
    # ==================== NATIVE VECTOR SEARCH ====================
    
    def vector_search_persons(self, query_embedding: List[float], limit: int = 10, min_score: float = 0.5) -> List[dict]:
        """
        Search persons menggunakan Neo4j NATIVE Vector Index.
        Ini yang seharusnya dipakai - jauh lebih cepat!
        """
        with self.driver.session(database=self.db) as session:
            result = session.run("""
                CALL db.index.vector.queryNodes('person_embedding_index', $limit_candidates, $embedding)
                YIELD node AS p, score
                WHERE score >= $min_score
                
                // Get related data
                OPTIONAL MATCH (p)-[:HELD_POSITION]->(pos:Position)
                OPTIONAL MATCH (p)-[:BORN_IN]->(city:City)-[:LOCATED_IN]->(country:Country)
                OPTIONAL MATCH (p)-[:DIED_IN]->(death_city:City)
                
                WITH p, score,
                     collect(DISTINCT coalesce(pos.label, pos.name))[..5] AS positions,
                     collect(DISTINCT country.country)[0] AS birth_country,
                     death_city.city AS death_place
                
                RETURN 
                    elementId(p) AS element_id,
                    p.article_id AS article_id,
                    p.full_name AS name,
                    p.description AS description,
                    p.abstract AS abstract,
                    p.image_url AS image,
                    p.birth_date AS birth_date,
                    p.death_date AS death_date,
                    death_place,
                    score AS similarity_score,
                    positions,
                    birth_country AS country
                ORDER BY score DESC
                LIMIT $limit
            """, {
                "embedding": query_embedding,
                "limit_candidates": limit * 2,  # Get more candidates for filtering
                "min_score": min_score,
                "limit": limit
            })
            
            return [dict(r) for r in result]
    
    def vector_search_events(self, query_embedding: List[float], limit: int = 10, min_score: float = 0.5) -> List[dict]:
        """
        Search events menggunakan Neo4j NATIVE Vector Index.
        """
        with self.driver.session(database=self.db) as session:
            result = session.run("""
                CALL db.index.vector.queryNodes('event_embedding_index', $limit_candidates, $embedding)
                YIELD node AS e, score
                WHERE score >= $min_score
                
                OPTIONAL MATCH (e)-[:HELD_IN]->(country:Country)
                
                WITH e, score,
                     collect(DISTINCT country.country)[0] AS event_country
                
                RETURN 
                    elementId(e) AS element_id,
                    e.event_id AS event_id,
                    e.name AS name,
                    e.description AS description,
                    e.image_url AS image,
                    e.impact AS impact,
                    e.start_date AS start_date,
                    e.end_date AS end_date,
                    score AS similarity_score,
                    event_country AS country
                ORDER BY score DESC
                LIMIT $limit
            """, {
                "embedding": query_embedding,
                "limit_candidates": limit * 2,
                "min_score": min_score,
                "limit": limit
            })
            
            return [dict(r) for r in result]
    
    def find_similar_persons(self, person_element_id: str, limit: int = 10, min_score: float = 0.5) -> List[dict]:
        """
        Find similar persons berdasarkan embedding seseorang.
        Pakai Native Vector Index.
        """
        with self.driver.session(database=self.db) as session:
            # Get embedding dari source person
            source = session.run("""
                MATCH (p:Person)
                WHERE elementId(p) = $element_id
                RETURN p.embedding AS embedding, p.full_name AS name
            """, {"element_id": person_element_id})
            
            record = source.single()
            if not record or not record["embedding"]:
                return []
            
            source_embedding = record["embedding"]
            source_name = record["name"]
            
            # Search similar using vector index
            result = session.run("""
                CALL db.index.vector.queryNodes('person_embedding_index', $limit_candidates, $embedding)
                YIELD node AS p, score
                WHERE elementId(p) <> $exclude_id AND score >= $min_score
                
                OPTIONAL MATCH (p)-[:HELD_POSITION]->(pos:Position)
                OPTIONAL MATCH (p)-[:BORN_IN]->(city:City)-[:LOCATED_IN]->(country:Country)
                
                WITH p, score,
                     collect(DISTINCT coalesce(pos.label, pos.name))[..5] AS positions,
                     collect(DISTINCT country.country)[0] AS birth_country
                
                RETURN 
                    elementId(p) AS element_id,
                    p.full_name AS name,
                    p.description AS description,
                    p.image_url AS image,
                    score AS similarity_score,
                    positions,
                    birth_country AS country
                ORDER BY score DESC
                LIMIT $limit
            """, {
                "embedding": source_embedding,
                "exclude_id": person_element_id,
                "limit_candidates": limit * 2,
                "min_score": min_score,
                "limit": limit
            })
            
            return {
                "source": {"element_id": person_element_id, "name": source_name},
                "similar": [dict(r) for r in result]
            }
    
    def find_similar_events(self, event_element_id: str, limit: int = 10, min_score: float = 0.5) -> List[dict]:
        """Find similar events berdasarkan embedding."""
        with self.driver.session(database=self.db) as session:
            source = session.run("""
                MATCH (e:Event)
                WHERE elementId(e) = $element_id
                RETURN e.embedding AS embedding, e.name AS name
            """, {"element_id": event_element_id})
            
            record = source.single()
            if not record or not record["embedding"]:
                return []
            
            result = session.run("""
                CALL db.index.vector.queryNodes('event_embedding_index', $limit_candidates, $embedding)
                YIELD node AS e, score
                WHERE elementId(e) <> $exclude_id AND score >= $min_score
                
                OPTIONAL MATCH (e)-[:HELD_IN]->(country:Country)
                
                WITH e, score,
                     collect(DISTINCT country.country)[0] AS event_country
                
                RETURN 
                    elementId(e) AS element_id,
                    e.name AS name,
                    e.description AS description,
                    e.image_url AS image,
                    e.impact AS impact,
                    score AS similarity_score,
                    event_country AS country
                ORDER BY score DESC
                LIMIT $limit
            """, {
                "embedding": record["embedding"],
                "exclude_id": event_element_id,
                "limit_candidates": limit * 2,
                "min_score": min_score,
                "limit": limit
            })
            
            return {
                "source": {"element_id": event_element_id, "name": record["name"]},
                "similar": [dict(r) for r in result]
            }
    
    # ==================== STORAGE METHODS ====================
    
    def store_person_embedding(self, article_id: int, embedding: List[float], searchable_text: str = None):
        """Store embedding ke Person node"""
        with self.driver.session(database=self.db) as session:
            session.run("""
                MATCH (p:Person {article_id: $article_id})
                SET p.embedding = $embedding,
                    p.searchable_text = $searchable_text,
                    p.embedding_updated = datetime()
            """, {
                "article_id": article_id,
                "embedding": embedding,
                "searchable_text": searchable_text
            })
    
    def store_event_embedding(self, event_id: int, embedding: List[float], searchable_text: str = None):
        """Store embedding ke Event node"""
        with self.driver.session(database=self.db) as session:
            session.run("""
                MATCH (e:Event {event_id: $event_id})
                SET e.embedding = $embedding,
                    e.searchable_text = $searchable_text,
                    e.embedding_updated = datetime()
            """, {
                "event_id": event_id,
                "embedding": embedding,
                "searchable_text": searchable_text
            })

    def get_persons_without_embedding(self, limit: int = 100):
        """Get persons yang belum punya embedding - dengan SEMUA field yang tersedia"""
        with self.driver.session(database=self.db) as session:
            result = session.run("""
                MATCH (p:Person)
                WHERE p.embedding IS NULL 
                    AND p.article_id IS NOT NULL
                    AND p.full_name IS NOT NULL
                    AND trim(p.full_name) <> ''
                    AND p.embedding_failed IS NULL
                
                // Get related positions
                OPTIONAL MATCH (p)-[:HELD_POSITION]->(pos:Position)
                
                WITH p, collect(DISTINCT coalesce(pos.label, pos.name)) AS positions
                
                RETURN 
                    p.article_id AS article_id,
                    p.full_name AS full_name,
                    p.sex AS sex,
                    p.birth_year AS birth_year,
                    p.death_year AS death_year,
                    p.city AS city,
                    p.state AS state,
                    p.country AS country,
                    p.continent AS continent,
                    p.occupation AS occupation,
                    p.industry AS industry,
                    p.domain AS domain,
                    p.description AS description,
                    p.abstract AS abstract,
                    p.death_place AS death_place,
                    p.cause_of_death AS cause_of_death,
                    positions
                LIMIT $limit
            """, {"limit": limit})
            return [dict(r) for r in result]

    def get_events_without_embedding(self, limit: int = 100):
        """Get events yang belum punya embedding"""
        with self.driver.session(database=self.db) as session:
            result = session.run("""
                MATCH (e:Event)
                WHERE e.embedding IS NULL 
                    AND e.event_id IS NOT NULL
                    AND e.name IS NOT NULL
                    AND trim(e.name) <> ''
                    AND e.embedding_failed IS NULL
                RETURN e.event_id AS event_id,
                       e.name AS name,
                       e.description AS description,
                       e.impact AS impact
                LIMIT $limit
            """, {"limit": limit})
            return [dict(r) for r in result]
    
    def mark_embedding_failed(self, article_id: int, reason: str = None):
        """Mark person sebagai gagal embedding"""
        with self.driver.session(database=self.db) as session:
            session.run("""
                MATCH (p:Person {article_id: $article_id})
                SET p.embedding_failed = true,
                    p.embedding_failed_reason = $reason
            """, {"article_id": article_id, "reason": reason})
    
    def mark_event_embedding_failed(self, event_id: int, reason: str = None):
        """Mark event sebagai gagal embedding"""
        with self.driver.session(database=self.db) as session:
            session.run("""
                MATCH (e:Event {event_id: $event_id})
                SET e.embedding_failed = true,
                    e.embedding_failed_reason = $reason
            """, {"event_id": event_id, "reason": reason})
    
    def get_embedding_stats(self) -> dict:
        """Get statistics embeddings"""
        with self.driver.session(database=self.db) as session:
            result = session.run("""
                MATCH (p:Person)
                WITH count(p) AS total_persons,
                     sum(CASE WHEN p.embedding IS NOT NULL THEN 1 ELSE 0 END) AS persons_with_embedding
                MATCH (e:Event)
                RETURN total_persons,
                       persons_with_embedding,
                       count(e) AS total_events,
                       sum(CASE WHEN e.embedding IS NOT NULL THEN 1 ELSE 0 END) AS events_with_embedding
            """)
            return dict(result.single())


# Singleton instance
_vector_repo = None

def get_vector_repo() -> VectorRepository:
    global _vector_repo
    if _vector_repo is None:
        _vector_repo = VectorRepository()
    return _vector_repo