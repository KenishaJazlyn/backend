# neo4j_repo.py
import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")                     # e.g. neo4j+s://xxxx.databases.neo4j.io
NEO4J_USER = os.getenv("NEO4J_USERNAME")
NEO4J_PASS = os.getenv("NEO4J_PASSWORD")
NEO4J_DB   = os.getenv("NEO4J_DATABASE", "neo4j")      # default aura db = "neo4j"

# Optional (tidak wajib, tapi boleh)
AURA_INSTANCEID = os.getenv("AURA_INSTANCEID")
AURA_INSTANCENAME = os.getenv("AURA_INSTANCENAME")

# Create Aura driver
driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASS)
)

class Neo4jRepo:
    def __init__(self, driver):
        self.driver = driver
        self.db = NEO4J_DB     # simpan nama database

    def close(self):
        self.driver.close()

    # -------------------------
    # GET ALL PERSONS
    # -------------------------
    def get_all_persons(self, limit=1000):
        with self.driver.session(database=self.db) as session:
            res = session.run("""
                MATCH (p:Person)
                RETURN p.name AS name, p.article_id AS article_id, p.full_name AS full_name
                LIMIT $limit
            """, {"limit": limit})
            return [dict(r) for r in res]

    # -------------------------
    # FIND PERSON BY NAME
    # -------------------------
    def find_person_by_name(self, name):
        with self.driver.session(database=self.db) as session:
            res = session.run("""
                MATCH (p:Person {name: $name})
                RETURN p LIMIT 1
            """, {"name": name})
            row = res.single()
            return row["p"] if row else None

    # -------------------------
    # UPSERT ENRICHED DATA
    # -------------------------
    def upsert_person_enrichment(
        self,
        person_id,
        qid,
        description=None,
        image=None,
        cause=None,
        killer=None,
        reigns=None,
        dynasties=None,
        events=None
    ):
        with self.driver.session(database=self.db) as session:

            # Update basic attributes
            session.run("""
                MATCH (p:Person {article_id: $person_id})
                SET p.wikidata_qid = $qid,
                    p.abstract = $description,
                    p.image_url = $image,
                    p.cause_of_death = $cause
            """, {
                "person_id": person_id,
                "qid": qid,
                "description": description,
                "image": image,
                "cause": cause
            })

            # Killer relation
            if killer:
                session.run("""
                    MATCH (victim:Person {article_id: $person_id})
                    MERGE (killer:Person {name: $killer})
                    MERGE (victim)-[:KILLED_BY]->(killer)
                """, {"person_id": person_id, "killer": killer})

            # Positions (P39)
            if reigns:
                for r in reigns:
                    session.run("""
                        MATCH (p:Person {article_id: $person_id})
                        MERGE (pos:Position {name: $positionName})
                        MERGE (p)-[rel:HELD_POSITION]->(pos)
                        SET rel.start = $start,
                            rel.end = $end
                    """, {
                        "person_id": person_id,
                        "positionName": r.get("position_label") or "Unknown Position",
                        "start": r.get("start"),
                        "end": r.get("end")
                    })

            # Dynasties
            if dynasties:
                for d in dynasties:
                    if d and d.strip():  # pastikan tidak kosong
                        session.run("""
                            MATCH (p:Person {article_id: $person_id})
                            MERGE (dyn:Dynasty {name: $dynasty})
                            MERGE (p)-[:MEMBER_OF_DYNASTY]->(dyn)
                        """, {"person_id": person_id, "dynasty": d.strip()})
                        print(f"Created MEMBER_OF_DYNASTY relationship to: {d}")

            # Event participation
            if events:
                for e in events:
                    session.run("""
                        MATCH (p:Person {article_id: $person_id})
                        MERGE (ev:Event {name: $event})
                        MERGE (p)-[:PARTICIPATED_IN]->(ev)
                    """, {"person_id": person_id, "event": e})

    # -------------------------
    # GET ALL EVENTS
    # -------------------------
    def get_all_events(self, limit=1000):
        with self.driver.session(database=self.db) as session:
            res = session.run("""
                MATCH (e:Event)
                RETURN e.name AS name, e.event_id AS event_id
                LIMIT $limit
            """, {"limit": limit})
            return [dict(r) for r in res]
        
    # -------------------------
    # UPSERT EVENT ENRICHED DATA
    # -------------------------
    def upsert_event_enrichment(
        self,
        event_id,
        qid,
        description=None,
        image=None
    ):
        with self.driver.session(database=self.db) as session:

            # Update basic attributes
            session.run("""
                MATCH (e:Event {event_id: $event_id})
                SET e.wikidata_qid = $qid,
                    e.description = $description,
                    e.image_url = $image
            """, {
                "event_id": event_id,
                "qid": qid,
                "description": description,
                "image": image
            })

def get_repo():
    return Neo4jRepo(driver)
