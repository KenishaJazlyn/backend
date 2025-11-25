import requests
from urllib.parse import urlencode
import time

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": "KG-Enrichment/1.0 (student project)",
    "Accept": "application/sparql-results+json"
}

def run_sparql(endpoint, query, timeout=30, retries=3, backoff=1.0):
    params = {"query": query}
    url = endpoint + "?" + urlencode(params)
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if resp.status_code in (429, 503):
                time.sleep(backoff * (2 ** attempt))
                continue
            raise
        except requests.exceptions.RequestException:
            time.sleep(backoff * (2 ** attempt))
    raise RuntimeError("SPARQL query failed after retries")

def find_qid_by_label(name, limit=5):
    q = '''
    SELECT ?person WHERE {
      ?person rdfs:label "%s"@en .
    } LIMIT %d
    ''' % (name.replace('"','\\"'), limit)
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    results = data.get('results', {}).get('bindings', [])
    qids = [r['person']['value'].split('/')[-1] for r in results]
    return qids

def get_person_basic_by_qid(qid):
    q = '''
    SELECT ?description ?image WHERE {
      BIND(wd:%s AS ?person)
      OPTIONAL { ?person schema:description ?description FILTER(LANG(?description)='en') }
      OPTIONAL { ?person wdt:P18 ?image. }
    }
    ''' % qid
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    rows = data.get('results', {}).get('bindings', [])
    if not rows:
        return None
    row = rows[0]
    return {
        "qid": qid,
        "description": row.get("description", {}).get("value"),
        "image": row.get("image", {}).get("value")
    }

def get_person_positions(qid):
    q = '''
    SELECT ?positionLabel ?start ?end WHERE {
      BIND(wd:%s AS ?person)
      ?person p:P39 ?stmt .
      ?stmt ps:P39 ?position .
      OPTIONAL { ?stmt pq:P580 ?start. }
      OPTIONAL { ?stmt pq:P582 ?end. }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ''' % qid
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    rows = data.get('results', {}).get('bindings', [])
    out = []
    for r in rows:
        out.append({
            "position_label": r.get("positionLabel", {}).get("value"),
            "start": r.get("start", {}).get("value"),
            "end": r.get("end", {}).get("value")
        })
    return out

def get_person_dynasty(qid):
    q = '''
    SELECT ?dynastyLabel WHERE {
      BIND(wd:%s AS ?person)
      {
        ?person wdt:P53 ?dynasty.
      } UNION {
        ?person wdt:P103 ?dynasty.
      }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ''' % qid
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    rows = data.get('results', {}).get('bindings', [])
    dyns = []
    for r in rows:
        if 'dynastyLabel' in r:
            label = r['dynastyLabel']['value']
            if not label.startswith('Q'):
                dyns.append(label)
    return dyns

def get_person_cause_and_killer(qid):
    """FIXED: Use P157 for killer, not P119 (burial place)"""
    q = '''
    SELECT ?causeLabel ?killerLabel WHERE {
      BIND(wd:%s AS ?person)
      OPTIONAL { ?person wdt:P509 ?cause. }
      OPTIONAL { ?person wdt:P157 ?killer. }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ''' % qid
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    rows = data.get('results', {}).get('bindings', [])
    out = {"cause": None, "killer": None}
    if rows:
        r = rows[0]
        if 'causeLabel' in r: out['cause'] = r['causeLabel']['value']
        if 'killerLabel' in r: out['killer'] = r['killerLabel']['value']
    return out

def get_person_events(qid):
    q = '''
    SELECT ?eventLabel WHERE {
      BIND(wd:%s AS ?person)
      OPTIONAL { ?person wdt:P1344 ?event. }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ''' % qid
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    rows = data.get('results', {}).get('bindings', [])
    events = [r['eventLabel']['value'] for r in rows if 'eventLabel' in r]
    return events


# event enrichment
def get_event_qid_by_name(name, limit=1):
    q = '''
    SELECT ?event WHERE {
      ?event rdfs:label "%s"@en .
    } LIMIT %d
    ''' % (name.replace('"','\\"'), limit)
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    results = data.get('results', {}).get('bindings', [])
    qids = [r['event']['value'].split('/')[-1] for r in results]
    return qids

def get_event_basic_by_qid(qid):
    q = '''
    SELECT ?description ?image WHERE {
      BIND(wd:%s AS ?event)
      OPTIONAL { ?event schema:description ?description FILTER(LANG(?description)='en') }
      OPTIONAL { ?event wdt:P18 ?image. }
    }
    ''' % qid
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    rows = data.get('results', {}).get('bindings', [])
    if not rows:
        return None

    row = rows[0]
    return {
        "qid": qid,
        "description": row.get("description", {}).get("value"),
        "image": row.get("image", {}).get("value")
    }
def get_person_death_info(qid):
    """Get death date and place (P570, P20)"""
    q = '''
    SELECT ?deathDate ?deathPlaceLabel WHERE {
      BIND(wd:%s AS ?person)
      OPTIONAL { ?person wdt:P570 ?deathDate. }
      OPTIONAL { ?person wdt:P20 ?deathPlace. }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ''' % qid
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    rows = data.get('results', {}).get('bindings', [])
    if rows:
        r = rows[0]
        return {
            "death_date": r.get("deathDate", {}).get("value"),
            "death_place": r.get("deathPlaceLabel", {}).get("value")
        }
    return {}

def get_person_conflicts(qid):
    """Get military conflicts/wars participated in (P607)"""
    q = '''
    SELECT ?conflictLabel ?startTime ?endTime WHERE {
      BIND(wd:%s AS ?person)
      ?person wdt:P607 ?conflict.
      OPTIONAL { ?conflict wdt:P580 ?startTime. }
      OPTIONAL { ?conflict wdt:P582 ?endTime. }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ''' % qid
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    rows = data.get('results', {}).get('bindings', [])
    conflicts = []
    for r in rows:
        if 'conflictLabel' in r:
            conflicts.append({
                "conflict": r['conflictLabel']['value'],
                "start": r.get('startTime', {}).get('value'),
                "end": r.get('endTime', {}).get('value')
            })
    return conflicts

def get_person_awards(qid):
    """Get awards and honors received (P166)"""
    q = '''
    SELECT ?awardLabel ?year WHERE {
      BIND(wd:%s AS ?person)
      ?person p:P166 ?stmt.
      ?stmt ps:P166 ?award.
      OPTIONAL { ?stmt pq:P585 ?year. }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ''' % qid
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    rows = data.get('results', {}).get('bindings', [])
    awards = []
    for r in rows:
        if 'awardLabel' in r:
            awards.append({
                "award": r['awardLabel']['value'],
                "year": r.get('year', {}).get('value')
            })
    return awards

def get_person_notable_works(qid):
    """Get notable works/publications (P800)"""
    q = '''
    SELECT ?workLabel ?year WHERE {
      BIND(wd:%s AS ?person)
      ?person wdt:P800 ?work.
      OPTIONAL { ?work wdt:P577 ?year. }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ''' % qid
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    rows = data.get('results', {}).get('bindings', [])
    works = []
    for r in rows:
        if 'workLabel' in r:
            works.append({
                "work": r['workLabel']['value'],
                "year": r.get('year', {}).get('value')
            })
    return works

def get_person_alliances(qid):
    """Get political alliances or parties (P102)"""
    q = '''
    SELECT ?partyLabel ?startTime ?endTime WHERE {
      BIND(wd:%s AS ?person)
      ?person p:P102 ?stmt.
      ?stmt ps:P102 ?party.
      OPTIONAL { ?stmt pq:P580 ?startTime. }
      OPTIONAL { ?stmt pq:P582 ?endTime. }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ''' % qid
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    rows = data.get('results', {}).get('bindings', [])
    parties = []
    for r in rows:
        if 'partyLabel' in r:
            parties.append({
                "party": r['partyLabel']['value'],
                "start": r.get('startTime', {}).get('value'),
                "end": r.get('endTime', {}).get('value')
            })
    return parties

def get_person_military_rank(qid):
    """Get military ranks held (P410)"""
    q = '''
    SELECT ?rankLabel WHERE {
      BIND(wd:%s AS ?person)
      ?person wdt:P410 ?rank.
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ''' % qid
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    rows = data.get('results', {}).get('bindings', [])
    ranks = [r['rankLabel']['value'] for r in rows if 'rankLabel' in r]
    return ranks

def get_person_religious_orders(qid):
    """Get religious orders (P611)"""
    q = '''
    SELECT ?orderLabel WHERE {
      BIND(wd:%s AS ?person)
      ?person wdt:P611 ?order.
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ''' % qid
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    rows = data.get('results', {}).get('bindings', [])
    orders = [r['orderLabel']['value'] for r in rows if 'orderLabel' in r]
    return orders

def get_person_convicted_of(qid):
    """Get crimes convicted of (P1399)"""
    q = '''
    SELECT ?crimeLabel WHERE {
      BIND(wd:%s AS ?person)
      ?person wdt:P1399 ?crime.
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ''' % qid
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    rows = data.get('results', {}).get('bindings', [])
    crimes = [r['crimeLabel']['value'] for r in rows if 'crimeLabel' in r]
    return crimes