from fastapi import APIRouter, HTTPException
from app.services.enrichment.country_enrichment import (
    fix_country_continent_relationships,
    check_duplicate_country_continents
)

router = APIRouter()

@router.get("/check-duplicates")
def check_duplicates():
    """Check which countries have multiple continents"""
    duplicates = check_duplicate_country_continents()
    return {
        "total_duplicates": len(duplicates),
        "duplicates": duplicates
    }

@router.post("/fix-continents")
def fix_continents():
    """Fix country-continent relationships using Wikidata"""
    results = fix_country_continent_relationships()
    
    success_count = sum(1 for r in results if r.get("status") == "updated")
    error_count = sum(1 for r in results if r.get("status") == "error")
    not_found_count = sum(1 for r in results if r.get("status") == "not_found_in_wikidata")
    
    return {
        "total_processed": len(results),
        "success": success_count,
        "errors": error_count,
        "not_found": not_found_count,
        "results": results
    }