import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USERNAME")
NEO4J_PASS = os.getenv("NEO4J_PASSWORD")
NEO4J_DB   = os.getenv("NEO4J_DATABASE", "neo4j")

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASS)
)

class PersonRepo:
    def __init__(self, driver):
        self.driver = driver
        self.db = NEO4J_DB

    def get_all_persons(self, limit=1000):
        with self.driver.session(database=self.db) as session:
            res = session.run("""
                MATCH (p:Person)
                RETURN p.name AS name, p.article_id AS article_id, p.full_name AS full_name
                LIMIT $limit
            """, {"limit": limit})
            return [dict(r) for r in res]

    def find_person_by_name(self, name):
        with self.driver.session(database=self.db) as session:
            res = session.run("""
                MATCH (p:Person {name: $name})
                RETURN p LIMIT 1
            """, {"name": name})
            row = res.single()
            return row["p"] if row else None

    def find_person_by_full_name(self, full_name: str):
        """Find person by full_name (case-insensitive) - EFFICIENT single query"""
        with self.driver.session(database=self.db) as session:
            res = session.run("""
                MATCH (p:Person)
                WHERE toLower(p.full_name) = toLower($full_name)
                RETURN p.name AS name, p.article_id AS article_id, p.full_name AS full_name
                LIMIT 1
            """, {"full_name": full_name})
            row = res.single()
            return dict(row) if row else None

    def upsert_person_enrichment(
        self,
        person_id,
        qid,
        description=None,
        image=None,
        death_date=None,
        death_place=None,
        cause=None,
        killer=None,
        reigns=None,
        dynasties=None,
        events=None,
        conflicts=None,
        awards=None,
        works=None,
        alliances=None,
        ranks=None,
        orders=None,
        crimes=None
    ):
        reigns = reigns or []
        dynasties = dynasties or []
        events = events or []
        conflicts = conflicts or []
        awards = awards or []
        works = works or []
        alliances = alliances or []
        ranks = ranks or []
        orders = orders or []
        crimes = crimes or []

        with self.driver.session(database=self.db) as session:

            # Update basic person attributes
            session.run("""
                MATCH (p:Person {article_id: $person_id})
                SET p.wikidata_qid = $qid,
                    p.description = $description,
                    p.image_url = $image,
                    p.death_date = $death_date,
                    p.death_place = $death_place,
                    p.cause_of_death = $cause
            """, {
                "person_id": person_id,
                "qid": qid,
                "description": description,
                "image": image,
                "death_date": death_date,
                "death_place": death_place,
                "cause": cause
            })

            # Killer relationship
            if killer:
                session.run("""
                    MATCH (victim:Person {article_id: $person_id})
                    MERGE (k:Person {full_name: $killer})
                    MERGE (victim)-[:KILLED_BY]->(k)
                """, {"person_id": person_id, "killer": killer})

            # Positions (P39)
            if reigns:
                session.run("""
                    MATCH (p:Person {article_id:$person_id})
                    UNWIND $reigns AS pos
                    WITH p, pos
                    WHERE pos.position_label IS NOT NULL
                    MERGE (posNode:Position {label: pos.position_label})
                    MERGE (p)-[r:HELD_POSITION]->(posNode)
                    SET r.start = pos.start, r.end = pos.end
                """, {"person_id": person_id, "reigns": reigns})

            # Conflicts
            if conflicts:
                session.run("""
                    MATCH (p:Person {article_id:$person_id})
                    UNWIND $conflicts AS c
                    WITH p, c
                    WHERE c.conflict IS NOT NULL
                    MERGE (conf:Conflict {name: c.conflict})
                    MERGE (p)-[r:PARTICIPATED_IN_CONFLICT]->(conf)
                    SET r.start = c.start, r.end = c.end
                """, {"person_id": person_id, "conflicts": conflicts})

            # Awards
            if awards:
                session.run("""
                    MATCH (p:Person {article_id:$person_id})
                    UNWIND $awards AS a
                    WITH p, a
                    WHERE a.award IS NOT NULL
                    MERGE (aw:Award {name: a.award})
                    MERGE (p)-[r:RECEIVED_AWARD]->(aw)
                    SET r.year = a.year
                """, {"person_id": person_id, "awards": awards})

            # Notable Works
            if works:
                session.run("""
                    MATCH (p:Person {article_id:$person_id})
                    UNWIND $works AS w
                    WITH p, w
                    WHERE w.work IS NOT NULL
                    MERGE (wk:Work {title: w.work})
                    SET wk.year = w.year
                    WITH p, wk, w
                    MERGE (p)-[r:CREATED_WORK]->(wk)
                    SET r.year = w.year
                """, {"person_id": person_id, "works": works})

            # Political Alliances / Parties
            if alliances:
                session.run("""
                    MATCH (p:Person {article_id:$person_id})
                    UNWIND $alliances AS al
                    WITH p, al
                    WHERE al.party IS NOT NULL
                    MERGE (pa:Party {name: al.party})
                    MERGE (p)-[r:MEMBER_OF]->(pa)
                    SET r.start = al.start, r.end = al.end
                """, {"person_id": person_id, "alliances": alliances})

def get_person_repo():
    return PersonRepo(driver)