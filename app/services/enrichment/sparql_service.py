import requests
from urllib.parse import urlencode
import time
import random

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": "KG-Enrichment/1.0 (student project; educational use)",
    "Accept": "application/sparql-results+json"
}

def run_sparql(endpoint, query, timeout=30, retries=5, backoff=2.0):
    """Run SPARQL query with exponential backoff and jitter"""
    params = {"query": query}
    url = endpoint + "?" + urlencode(params)
    
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if resp.status_code in (429, 503, 500):
                # Rate limited or server error - wait longer
                wait_time = backoff * (2 ** attempt) + random.uniform(0, 1)
                print(f"⏳ Wikidata rate limit (attempt {attempt+1}/{retries}), waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue
            raise
        except requests.exceptions.RequestException as e:
            wait_time = backoff * (2 ** attempt) + random.uniform(0, 1)
            print(f"⚠️ Request error (attempt {attempt+1}/{retries}): {e}, waiting {wait_time:.1f}s...")
            time.sleep(wait_time)
    
    print(f"❌ SPARQL query failed after {retries} retries")
    return None  # Return None instead of raising, so enrichment can continue

def find_qid_by_label(name, limit=5):
    q = '''
    SELECT ?person WHERE {
      ?person rdfs:label "%s"@en .
    } LIMIT %d
    ''' % (name.replace('"','\\"'), limit)
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    if data is None:
        return []  # Return empty if SPARQL failed
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

def get_event_optional_enrichment(qid):
    q = '''
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX schema: <http://schema.org/>

    SELECT 
        ?description ?image ?startDate ?endDate ?coordinates 
        ?primaryCategory ?location ?deaths ?cause ?effect ?video
        ?participant ?partOf ?pointInTime ?describedBySource ?describedAtURL
        ?commonsCategory ?mainCategory ?detailMap ?pageBanner
        ?focusList ?hasPart
    WHERE 
    {
        BIND(wd:%s AS ?event)
        OPTIONAL { ?event schema:description ?description FILTER(LANG(?description)='en') }
        OPTIONAL { ?event wdt:P18 ?image. }
        OPTIONAL { ?event wdt:P580 ?startDate. } 
        OPTIONAL { ?event wdt:P582 ?endDate. } 
        OPTIONAL { ?event wdt:P625 ?coordinates. }
        
        # QID/Multi-Value Properties
        OPTIONAL { ?event wdt:P31 ?primaryCategory. }       # Instance of
        OPTIONAL { ?event wdt:P276 ?location. }            # Location
        OPTIONAL { ?event wdt:P828 ?cause. }               # Main Cause (Has Cause)
        OPTIONAL { ?event wdt:P1542 ?effect. }             # Cause Of (Has Effect)
        OPTIONAL { ?event wdt:P710 ?participant. }          
        OPTIONAL { ?event wdt:P361 ?partOf. }               # Part of (Already exists, but adding P-ID clarity)
        OPTIONAL { ?event wdt:P301 ?mainCategory. }
        OPTIONAL { ?event wdt:P1343 ?describedBySource. }
        
        # --- NEW PROPERTIES ---
        OPTIONAL { ?event wdt:P1013 ?focusList. }           # Focus list of Wikimedia project
        OPTIONAL { ?event wdt:P527 ?hasPart. }              # Has part(s)
        
        # Literal/URL Properties
        OPTIONAL { ?event wdt:P1120 ?deaths. }
        OPTIONAL { ?event wdt:P585 ?pointInTime. }
        OPTIONAL { ?event wdt:P373 ?commonsCategory. }
        OPTIONAL { ?event wdt:P948 ?pageBanner. }
        OPTIONAL { ?event wdt:P951 ?detailMap. }
        OPTIONAL { ?event wdt:P1047 ?video. }
        OPTIONAL { ?event wdt:P973 ?describedAtURL. }
    }
    ''' % qid
    
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    rows = data.get('results', {}).get('bindings', [])
    if not rows:
        return None

    # Use 'set' for all multi-value QID/URL properties
    result = {
        "qid": qid,
        "description": None,
        "image": None,
        "start_date": None,
        "end_date": None,
        "coordinates": None,
        "deaths": None,
        
        # Single Literal/URL/Date Properties
        "point_in_time": None,
        "commons_category": None,
        "page_banner": None,
        "detail_map": None,
        
        # Multi-Value QID/URL Properties (Initialized as sets)
        "primary_category_qids": set(), 
        "location_qids": set(),         
        "cause_qids": set(),            
        "effect_qids": set(),           
        "video_urls": set(),            
        "participant_qids": set(),      
        "part_of_qids": set(),          
        "described_by_source_qids": set(), 
        "described_at_url": set(),
        "main_category_qids": set(),
        
        # --- NEW PROPERTIES ---
        "focus_list_qids": set(),       # P1013
        "has_part_qids": set(),         # P527
    }
    
    # Define a mapping for multi-value fields to their Python dictionary keys
    multi_value_map = {
        "primaryCategory": "primary_category_qids",
        "location": "location_qids",
        "cause": "cause_qids",
        "effect": "effect_qids",
        "video": "video_urls",
        "participant": "participant_qids",
        "partOf": "part_of_qids",
        "describedBySource": "described_by_source_qids",
        "describedAtURL": "described_at_url",
        "mainCategory": "main_category_qids",
        # --- NEW MAPPINGS ---
        "focusList": "focus_list_qids",
        "hasPart": "has_part_qids",
    }

    for row in rows:
        # Single-Value Literals (Take the first one found)
        # Using concise check: assign if value is None AND row has the key
        if result["description"] is None and row.get("description"): result["description"] = row["description"]["value"]
        if result["image"] is None and row.get("image"): result["image"] = row["image"]["value"]
        if result["start_date"] is None and row.get("startDate"): result["start_date"] = row["startDate"]["value"]
        if result["end_date"] is None and row.get("endDate"): result["end_date"] = row["endDate"]["value"]
        if result["coordinates"] is None and row.get("coordinates"): result["coordinates"] = row["coordinates"]["value"]
        if result["deaths"] is None and row.get("deaths"): result["deaths"] = row["deaths"]["value"]
        if result["point_in_time"] is None and row.get("pointInTime"): result["point_in_time"] = row["pointInTime"]["value"]
        if result["commons_category"] is None and row.get("commonsCategory"): result["commons_category"] = row["commonsCategory"]["value"]
        if result["page_banner"] is None and row.get("pageBanner"): result["page_banner"] = row["pageBanner"]["value"]
        if result["detail_map"] is None and row.get("detailMap"): result["detail_map"] = row["detailMap"]["value"]

        # Multi-Value Fields (Collect all unique values into sets)
        for sparql_key, result_key in multi_value_map.items():
            if row.get(sparql_key):
                result[result_key].add(row[sparql_key]["value"])

    # Convert all sets to lists (or None if the set is empty)
    for key in list(result.keys()):
        if isinstance(result[key], set):
            if result[key]:
                result[key] = list(result[key])
            else:
                result[key] = None
    
    return result

def get_all_countries_continents():
    """Get all countries and their continents from Wikidata"""
    q = '''
    SELECT ?country ?countryLabel ?continentLabel WHERE {
      ?country wdt:P31/wdt:P279* wd:Q6256 .  # Instance of country
      ?country wdt:P30 ?continent .           # Located in continent
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    '''
    
    data = run_sparql(WIKIDATA_ENDPOINT, q)
    rows = data.get('results', {}).get('bindings', [])
    
    result = {}
    for row in rows:
        country_name = row['countryLabel']['value']
        continent_name = row['continentLabel']['value']
        
        # Skip if country name starts with 'Q' (unresolved labels)
        if not country_name.startswith('Q') and not continent_name.startswith('Q'):
            result[country_name] = continent_name
            
    return result