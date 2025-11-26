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

class EventRepo:
    def __init__(self, driver):
        self.driver = driver
        self.db = NEO4J_DB

    def get_all_events(self, limit=1000):
        with self.driver.session(database=self.db) as session:
            res = session.run("""
                MATCH (e:Event)
                RETURN e.name AS name, e.event_id AS event_id
                LIMIT $limit
            """, {"limit": limit})
            return [dict(r) for r in res]

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

    def upsert_event_enrichment_optional(
        self,
        event_id,
        qid,
        description=None,
        image=None,
        start_date=None, 
        end_date=None, 
        coordinates=None,
        
        # Properti Tunggal/Literal
        deaths=None,            
        point_in_time=None,
        commons_category=None,
        page_banner=None,
        detail_map=None,
        
        # Properti Multi-Nilai (List QID/URL)
        primary_category_qids=None, 
        location_qids=None, 
        cause_qids=None, 
        effect_qids=None, 
        video_urls=None, 
        participant_qids=None,
        part_of_qids=None,
        described_by_source_qids=None,
        described_at_url=None,
        main_category_qids=None,
        focus_list_qids=None,
        has_part_qids=None,
    ):
        with self.driver.session(database=self.db) as session:
            session.run("""
                MATCH (e:Event {event_id: $event_id})
                SET 
                    // Dasar & Temporal
                    e.wikidata_qid = $qid,
                    e.description = $description,
                    e.image_url = $image,
                    e.coordinates = $coordinates,
                    e.start_date = $start_date,
                    e.end_date = $end_date,
                    e.last_enriched = datetime(),
                    
                    // Numerik & Tanggal Tunggal
                    e.number_of_deaths = toInteger($deaths),
                    e.point_in_time = $point_in_time,
                    
                    // Literal Tunggal Media/Kategori
                    e.commons_category = $commons_category,
                    e.page_banner = $page_banner,
                    e.detail_map = $detail_map,
                    
                    // Multi-Nilai (List QID/URL)
                    e.primary_category_qids = $primary_category_qids,
                    e.location_qids = $location_qids,
                    e.cause_qids = $cause_qids,
                    e.effect_qids = $effect_qids,
                    e.video_urls = $video_urls,
                    e.participant_qids = $participant_qids,
                    e.part_of_qids = $part_of_qids,
                    e.described_by_source_qids = $described_by_source_qids,
                    e.described_at_url = $described_at_url,
                    e.main_category_qids = $main_category_qids,
                    e.focus_list_qids = $focus_list_qids,
                    e.has_part_qids = $has_part_qids
                    
                """, {
                    "event_id": event_id,
                    "qid": qid,
                    "description": description,
                    "image": image,
                    "start_date": start_date,
                    "end_date": end_date,
                    "coordinates": coordinates,
                    
                    # Properti Tunggal/Literal
                    "deaths": deaths,
                    "point_in_time": point_in_time,
                    "commons_category": commons_category,
                    "page_banner": page_banner,
                    "detail_map": detail_map,
                    
                    # Properti Multi-Nilai
                    "primary_category_qids": primary_category_qids,
                    "location_qids": location_qids,
                    "cause_qids": cause_qids,
                    "effect_qids": effect_qids,
                    "video_urls": video_urls,
                    "participant_qids": participant_qids,
                    "part_of_qids": part_of_qids,
                    "described_by_source_qids": described_by_source_qids,
                    "described_at_url": described_at_url,
                    "main_category_qids": main_category_qids,
                    "focus_list_qids": focus_list_qids,
                    "has_part_qids": has_part_qids,
                })


def get_event_repo():
    return EventRepo(driver)