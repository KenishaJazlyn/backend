from app.services.sparql_service import (
    find_qid_by_label,
    get_person_basic_by_qid,
    get_person_positions,
    get_person_dynasty,
    get_person_cause_and_killer,
    get_person_events,
    get_event_qid_by_name,
    get_event_basic_by_qid,
    get_event_optional_enrichment,
)
from app.db.neo4j_repo import get_repo

repo = get_repo()


def enrich_person_by_name(name):
    # Step A: find person in internal Neo4j
    persons = repo.get_all_persons(limit=10000)
    match = None

    for p in persons:
        internal_name = p.get("full_name")
        if internal_name and internal_name.lower() == name.lower():
            match = p
            break

    if not match:
        return {"status": "not_found", "name": name}

    person_id = match.get("article_id")  # ubah dari 'id' ke 'article_id'

    # Tambahkan validasi
    if person_id is None:
        return {"status": "error", "name": name, "message": "article_id is None"}

    print(f"Found internal person: {internal_name} with article_id {person_id}")

    # Step B: find QID in Wikidata
    qids = find_qid_by_label(name, limit=5)
    if not qids:
        return {"status": "qid_not_found", "name": name}

    qid = qids[0]  # naive: choose first; you can improve disambiguation

    # Step C: fetch enrichment pieces
    basic = get_person_basic_by_qid(qid)
    positions = get_person_positions(qid)
    dynasties = get_person_dynasty(qid)
    cod = get_person_cause_and_killer(qid)
    events = get_person_events(qid)

    # Step D: persist to Neo4j
    repo.upsert_person_enrichment(
        person_id=person_id,
        qid=qid,
        description=basic.get("description") if basic else None,
        image=basic.get("image") if basic else None,
        cause=cod.get("cause"),
        killer=cod.get("killer"),
        reigns=positions,
        dynasties=dynasties,
        events=events,
    )

    return {"status": "ok", "name": name, "qid": qid}


# event enrichment
def enrich_event_by_name(name):
    # Step A: find event in internal Neo4j
    events = repo.get_all_events(limit=10000)
    match = None

    for e in events:
        internal_name = e.get("name")
        if internal_name and internal_name.lower() == name.lower():
            match = e
            break

    if not match:
        return {"status": "not_found", "name": name}

    event_id = match.get("event_id")

    # Tambahkan validasi
    if event_id is None:
        return {"status": "error", "name": name, "message": "event_id is None"}

    print(f"Found internal event: {internal_name} with event_id {event_id}")

    # Step B: find QID in Wikidata
    qid = get_event_qid_by_name(name)
    if not qid:
        return {"status": "qid_not_found", "name": name}

    # Step C: fetch enrichment pieces
    basic = get_event_basic_by_qid(qid)

    # Step D: persist to Neo4j
    repo.upsert_event_enrichment(
        event_id=event_id,
        qid=qid,
        description=basic.get("description") if basic else None,
        image=basic.get("image") if basic else None,
    )

    return {"status": "ok", "name": name, "qid": qid}


def enrich_all_events():
    events = repo.get_all_events(limit=10000)
    results = []
    for e in events:
        name = e.get("name")
        event_id = e.get("event_id")
        if not name or not event_id:
            results.append({"event_id": event_id, "status": "skip_no_name_or_id"})
            continue

        qids = get_event_qid_by_name(name)
        qid = qids[0] if qids else None
        if not qid:
            results.append(
                {"event_id": event_id, "name": name, "status": "qid_not_found"}
            )
            continue

        basic = get_event_basic_by_qid(qid)
        repo.upsert_event_enrichment(
            event_id=event_id,
            qid=qid,
            description=basic.get("description") if basic else None,
            image=basic.get("image") if basic else None,
        )
        results.append({"event_id": event_id, "name": name, "qid": qid, "status": "ok"})
    return results

def enrich_events_with_optional_properties():
    events = repo.get_all_events(limit=10000)
    results = []
    
    for e in events:
        name = e.get("name")
        event_id = e.get("event_id")
        
        if not name or not event_id:
            results.append({"event_id": event_id, "status": "skip_no_name_or_id"})
            continue

        qids = get_event_qid_by_name(name)
        qid = qids[0] if qids else None
        
        if not qid:
            results.append(
                {"event_id": event_id, "name": name, "status": "qid_not_found"}
            )
            continue

        enriched_data = get_event_optional_enrichment(qid)
        
        if not enriched_data:
            results.append(
                {"event_id": event_id, "name": name, "qid": qid, "status": "enrichment_data_empty"}
            )
            continue
        
        try:
            # Panggilan fungsi upsert_event_enrichment_optional yang diperbarui
            repo.upsert_event_enrichment_optional(
                event_id=event_id,
                qid=qid,
                
                # --- PROPERTI DASAR ---
                description=enriched_data.get("description"),
                image=enriched_data.get("image"),
                start_date=enriched_data.get("start_date"),
                end_date=enriched_data.get("end_date"),
                coordinates=enriched_data.get("coordinates"),
                
                # --- PROPERTI BARU TUNGGAL/LITERAL ---
                deaths=enriched_data.get("deaths"),
                point_in_time=enriched_data.get("point_in_time"),
                commons_category=enriched_data.get("commons_category"),
                page_banner=enriched_data.get("page_banner"),
                detail_map=enriched_data.get("detail_map"),
                
                # --- PROPERTI MULTI-NILAI (LIST QID/URL) ---
                primary_category_qids=enriched_data.get("primary_category_qids"), 
                location_qids=enriched_data.get("location_qids"),
                cause_qids=enriched_data.get("cause_qids"),
                effect_qids=enriched_data.get("effect_qids"),
                video_urls=enriched_data.get("video_urls"),
                participant_qids=enriched_data.get("participant_qids"),
                part_of_qids=enriched_data.get("part_of_qids"),
                described_by_source_qids=enriched_data.get("described_by_source_qids"),
                described_at_url=enriched_data.get("described_at_url"),
                main_category_qids=enriched_data.get("main_category_qids"),
                focus_list_qids=enriched_data.get("focus_list_qids"),
                has_part_qids=enriched_data.get("has_part_qids"),

            )
            results.append({"event_id": event_id, "name": name, "qid": qid, "status": "ok"})
        except Exception as e:
            results.append({"event_id": event_id, "name": name, "qid": qid, "status": "upsert_failed", "error": str(e)})
            continue 
            
    return results