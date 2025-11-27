from app.services.enrichment.sparql_service import get_all_countries_continents
from app.db.neo4j_repo import get_repo

def fix_country_continent_relationships():
    """Fix duplicate country-continent relationships using Wikidata"""
    
    # Get correct mappings from Wikidata
    wikidata_mappings = get_all_countries_continents()
    
    repo = get_repo()
    results = []
    
    with repo.driver.session(database=repo.db) as session:
        # Get all countries in our database
        country_result = session.run("""
            MATCH (c:Country)
            RETURN c.country as country_name, id(c) as country_id
        """)
        
        countries = [dict(r) for r in country_result]
        
        for country in countries:
            country_name = country['country_name']
            country_id = country['country_id']
            
            if not country_name:
                continue
                
            # Find correct continent from Wikidata
            correct_continent = wikidata_mappings.get(country_name)
            
            if correct_continent:
                try:
                    # Remove all existing continent relationships
                    session.run("""
                        MATCH (c:Country)-[r:LOCATED_IN]->(cont:Continent)
                        WHERE id(c) = $country_id
                        DELETE r
                    """, {"country_id": country_id})
                    
                    # Create/connect to correct continent
                    session.run("""
                        MATCH (c:Country)
                        WHERE id(c) = $country_id
                        MERGE (cont:Continent {continent: $continent_name})
                        MERGE (c)-[:LOCATED_IN]->(cont)
                    """, {
                        "country_id": country_id,
                        "continent_name": correct_continent
                    })
                    
                    results.append({
                        "country": country_name,
                        "continent": correct_continent,
                        "status": "updated"
                    })
                    
                except Exception as e:
                    results.append({
                        "country": country_name,
                        "status": "error",
                        "error": str(e)
                    })
            else:
                results.append({
                    "country": country_name,
                    "status": "not_found_in_wikidata"
                })
    
    return results

def check_duplicate_country_continents():
    """Check which countries have multiple continents"""
    repo = get_repo()
    
    with repo.driver.session(database=repo.db) as session:
        result = session.run("""
            MATCH (c:Country)-[:LOCATED_IN]->(cont:Continent)
            WITH c, collect(cont.continent) as continents
            WHERE size(continents) > 1
            RETURN c.country as country, continents, size(continents) as continent_count
            ORDER BY continent_count DESC
        """)
        
        return [dict(r) for r in result]