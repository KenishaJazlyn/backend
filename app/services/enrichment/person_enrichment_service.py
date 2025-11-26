from app.services.enrichment.sparql_service import (
    find_qid_by_label, get_person_basic_by_qid, get_person_positions, 
    get_person_dynasty, get_person_cause_and_killer, get_person_events,
    get_person_death_info, get_person_conflicts, get_person_awards,
    get_person_notable_works, get_person_alliances, get_person_military_rank,
    get_person_religious_orders, get_person_convicted_of
)
from app.db.person_repo import get_person_repo

repo = get_person_repo()

def preview_person_enrichment(name):
    """
    Preview enrichment FOR A SINGLE QID (use qids[0] only).
    Does NOT write to the DB.
    """
    # Step A: find person in internal Neo4j
    persons = repo.get_all_persons(limit=10000)
    match = None
    for p in persons:
        internal_name = p.get('full_name')
        if internal_name and internal_name.lower() == name.lower():
            match = p
            break

    if not match:
        return {"status":"not_found", "name": name}

    person_id = match.get('article_id')
    if person_id is None:
        return {"status":"error", "name": name, "message": "article_id is None"}

    # Step B: get single QID via label search (take qids[0])
    qids = find_qid_by_label(name, limit=1)
    if not qids:
        return {"status":"qid_not_found", "name": name}
    qid = qids[0]
    basic = get_person_basic_by_qid(qid)
    # Step C: fetch enrichment for that single QID
    positions = get_person_positions(qid)
    dynasties = get_person_dynasty(qid)
    cod = get_person_cause_and_killer(qid)
    events = get_person_events(qid)
    death_info = get_person_death_info(qid)
    conflicts = get_person_conflicts(qid)
    awards = get_person_awards(qid)
    works = get_person_notable_works(qid)
    alliances = get_person_alliances(qid)
    ranks = get_person_military_rank(qid)
    orders = get_person_religious_orders(qid)
    crimes = get_person_convicted_of(qid)
            

    candidate = {
        "qid": qid,
        "wikidata_url": f"https://www.wikidata.org/wiki/{qid}",
        "description": basic.get('description') if basic else None,
        "image": basic.get('image') if basic else None,
        "positions": positions,
        "dynasties": dynasties,
        "cause_of_death": cod.get('cause'),
        "killer": cod.get('killer'),
        "events": events,
        "death_info": death_info,
        "conflicts": conflicts,
        "awards": awards,
        "notable_works": works,
        "alliances": alliances,
        "military_ranks": ranks,
        "religious_orders": orders,
        "convicted_of": crimes
    }

    return {"status":"ok", "name": name, "person_id": person_id, "candidate": candidate}

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

    qid = qids[0]
    basic = get_person_basic_by_qid(qid)
    # Step C: fetch enrichment for that single QID
    positions = get_person_positions(qid)
    dynasties = get_person_dynasty(qid)
    cod = get_person_cause_and_killer(qid)
    events = get_person_events(qid)
    death_info = get_person_death_info(qid)
    conflicts = get_person_conflicts(qid)
    awards = get_person_awards(qid)
    works = get_person_notable_works(qid)
    alliances = get_person_alliances(qid)
    ranks = get_person_military_rank(qid)
    orders = get_person_religious_orders(qid)
    crimes = get_person_convicted_of(qid)

    # Step D: persist to Neo4j
    repo.upsert_person_enrichment(
        person_id=person_id,
        qid=qid,
        description=basic.get('description') if basic else None,
        image=basic.get('image') if basic else None,
        death_date=death_info.get('death_date'),
        death_place=death_info.get('death_place'),
        cause=cod.get('cause'),
        killer=cod.get('killer'),
        reigns=positions,
        dynasties=dynasties,
        events=events,
        conflicts=conflicts,
        awards=awards,
        works=works,
        alliances=alliances,
        ranks=ranks,
        orders=orders,
        crimes=crimes
    )
    return {"status":"ok","name":name,"qid":qid}
