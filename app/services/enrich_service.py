from app.services.sparql_service import find_qid_by_label, get_person_basic_by_qid, get_person_positions, get_person_dynasty, get_person_cause_and_killer, get_person_events
from app.db.neo4j_repo import get_repo

repo = get_repo()

def enrich_person_by_name(name):
    # Step A: find person in internal Neo4j
    persons = repo.get_all_persons(limit=10000)
    match = None

    for p in persons:
        # cek name OR full_name
        internal_name = p.get('name') or p.get('full_name')

        if internal_name and internal_name.lower() == name.lower():
            match = p
            break

    if not match:
        return {"status":"not_found", "name": name}

    person_id = match.get('id') or match.get('name')

    # Step B: find QID in Wikidata
    qids = find_qid_by_label(name, limit=5)
    if not qids:
        return {"status":"qid_not_found", "name": name}

    qid = qids[0]  # naive: choose first; you can improve disambiguation

    # Step C: fetch enrichment pieces
    basic = get_person_basic_by_qid(qid)
    positions = get_person_positions(qid)
    dynasties = get_person_dynasty(qid)
    cod = get_person_cause_and_killer(qid)
    events = get_person_events(qid)

    # Step D: persist to Neo4j
    repo.upsert_person_enrichment(
        person_id = person_id,
        qid = qid,
        description = basic.get('description') if basic else None,
        image = basic.get('image') if basic else None,
        cause = cod.get('cause'),
        killer = cod.get('killer'),
        reigns = positions,
        dynasties = dynasties,
        events = events
    )

    return {"status":"ok","name":name,"qid":qid}
