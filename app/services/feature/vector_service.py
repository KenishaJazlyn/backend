import numpy as np
from sentence_transformers import SentenceTransformer
from typing import List, Optional
import os
import torch

# Singleton pattern untuk model
_model = None

# Model options (uncomment yang mau dipakai):
# - "all-MiniLM-L6-v2"          : Fast, 384 dim, tapi kurang bagus untuk context
# - "all-mpnet-base-v2"         : Better quality, 768 dim
# - "BAAI/bge-base-en-v1.5"     : Good for semantic search, 768 dim
# - "intfloat/e5-base-v2"       : Great for semantic search, 768 dim
# - "Alibaba-NLP/gte-base-en-v1.5" : Qwen-based, good quality, 768 dim

DEFAULT_MODEL = "BAAI/bge-base-en-v1.5"  # Recommended for semantic search


def get_embedding_model():
    """Load embedding model (singleton pattern)"""
    global _model
    if _model is None:
        model_name = os.getenv("EMBEDDING_MODEL", DEFAULT_MODEL)
        device = "cpu"
        
        print(f"🔄 Loading embedding model: {model_name}")
        
        try:
            _model = SentenceTransformer(model_name, device=device)
            print(f"✅ Model loaded successfully! Dimension: {_model.get_sentence_embedding_dimension()}")
        except Exception as e:
            print(f"❌ Error loading model {model_name}: {e}")
            print("⚠️ Falling back to all-MiniLM-L6-v2")
            _model = SentenceTransformer("all-MiniLM-L6-v2", device=device)
    
    return _model


def get_embedding_dimension() -> int:
    """Get dimension of current embedding model"""
    model = get_embedding_model()
    return model.get_sentence_embedding_dimension()


def generate_embedding(text: str) -> List[float]:
    """Generate embedding vector dari text"""
    if not text or not text.strip():
        return None
    
    try:
        model = get_embedding_model()
        embedding = model.encode(text, convert_to_numpy=True, show_progress_bar=False)
        return embedding.tolist()
    except Exception as e:
        print(f"Error generating embedding: {e}")
        return None


def generate_embeddings_batch(texts: List[str]) -> List[List[float]]:
    """Generate embeddings untuk multiple texts"""
    try:
        model = get_embedding_model()
        valid_texts = [t if t and t.strip() else "" for t in texts]
        embeddings = model.encode(valid_texts, convert_to_numpy=True, show_progress_bar=True)
        return [emb.tolist() for emb in embeddings]
    except Exception as e:
        print(f"Error generating batch embeddings: {e}")
        return [None] * len(texts)


def compute_similarity(embedding1: List[float], embedding2: List[float]) -> float:
    """Compute cosine similarity between two embeddings"""
    if not embedding1 or not embedding2:
        return 0.0
    
    try:
        vec1 = np.array(embedding1)
        vec2 = np.array(embedding2)
        
        dot_product = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return float(dot_product / (norm1 * norm2))
    except Exception as e:
        print(f"Error computing similarity: {e}")
        return 0.0


def create_searchable_text_person(person: dict) -> str:
    """
    Buat text yang akan di-embed untuk Person.
    
    ⚠️ PENTING: TIDAK INCLUDE NAMA!
    - Nama akan di-handle oleh keyword search
    - Embedding hanya untuk KONTEKS (occupation, era, location, dll)
    - Ini mencegah "John Jay" mirip dengan "Jay-Z" hanya karena ada kata "Jay"
    
    Contoh output:
    "male politician political leader statesman government official 
     worked in government public service from United States North America 
     born 1745 historical figure early era founding father colonial"
    """
    parts = []
    
    # ❌ TIDAK INCLUDE NAMA - nama di-handle keyword search
    # if person.get("full_name"):
    #     parts.append(person["full_name"])
    
    # 1. Gender
    if person.get("sex"):
        parts.append(person["sex"].lower())
    
    # 2. Occupation (SANGAT PENTING untuk semantic!)
    if person.get("occupation"):
        occupation = person["occupation"]
        parts.append(occupation)
        
        # Tambahkan variasi kata untuk semantic matching
        occupation_lower = occupation.lower()
        if "politician" in occupation_lower:
            parts.append("political leader statesman government official public servant legislator lawmaker")
        if "president" in occupation_lower:
            parts.append("head of state leader executive chief commander")
        if "military" in occupation_lower or "general" in occupation_lower or "soldier" in occupation_lower:
            parts.append("military commander army officer soldier warrior combat veteran")
        if "scientist" in occupation_lower:
            parts.append("researcher academic scholar professor intellectual")
        if "artist" in occupation_lower or "painter" in occupation_lower:
            parts.append("creative painter sculptor visual arts")
        if "writer" in occupation_lower or "author" in occupation_lower or "poet" in occupation_lower:
            parts.append("novelist poet literary author writer intellectual")
        if "athlete" in occupation_lower:
            parts.append("sports player sportsman athletic competitor")
        if "actor" in occupation_lower or "actress" in occupation_lower:
            parts.append("performer entertainer movie star theater film")
        if "musician" in occupation_lower or "singer" in occupation_lower or "rapper" in occupation_lower:
            parts.append("music artist performer composer singer entertainer")
        if "diplomat" in occupation_lower:
            parts.append("ambassador foreign affairs international relations negotiator")
        if "lawyer" in occupation_lower or "judge" in occupation_lower:
            parts.append("legal profession attorney justice court law")
        if "doctor" in occupation_lower or "physician" in occupation_lower:
            parts.append("medical profession healthcare physician healer")
        if "engineer" in occupation_lower:
            parts.append("technical profession technology innovation builder")
        if "business" in occupation_lower or "entrepreneur" in occupation_lower:
            parts.append("business commerce trade entrepreneur investor")
        if "king" in occupation_lower or "queen" in occupation_lower or "emperor" in occupation_lower:
            parts.append("royalty monarch ruler sovereign crown throne")
        if "religious" in occupation_lower or "priest" in occupation_lower or "pope" in occupation_lower:
            parts.append("religious leader clergy spiritual faith church")
    
    # 3. Industry
    if person.get("industry"):
        industry = person["industry"]
        parts.append(f"industry {industry}")
        
        industry_lower = industry.lower()
        if "government" in industry_lower:
            parts.append("public service civil servant politics administration")
        if "entertainment" in industry_lower:
            parts.append("show business media arts celebrity fame")
        if "sports" in industry_lower:
            parts.append("athletics competition games championship")
        if "business" in industry_lower:
            parts.append("commerce trade entrepreneur corporate")
        if "science" in industry_lower:
            parts.append("research academic discovery innovation")
        if "military" in industry_lower:
            parts.append("armed forces defense war combat")
        if "education" in industry_lower:
            parts.append("teaching academia school university professor")
        if "healthcare" in industry_lower or "medical" in industry_lower:
            parts.append("medicine hospital doctor treatment")
    
    # 4. Domain
    if person.get("domain"):
        domain = person["domain"]
        parts.append(f"domain {domain}")
        
        domain_lower = domain.lower()
        if "politics" in domain_lower or "institutions" in domain_lower:
            parts.append("governance leadership policy government state")
        if "arts" in domain_lower:
            parts.append("creative culture artistic expression")
        if "science" in domain_lower or "technology" in domain_lower:
            parts.append("innovation research discovery invention")
        if "sports" in domain_lower:
            parts.append("athletics competition champion victory")
        if "business" in domain_lower:
            parts.append("commerce economy trade finance")
        if "humanities" in domain_lower:
            parts.append("philosophy literature history culture")
    
    # 5. Location (birth place) - PENTING untuk konteks geografis
    location_parts = []
    if person.get("city"):
        location_parts.append(person["city"])
    if person.get("state"):
        location_parts.append(person["state"])
    if person.get("country"):
        location_parts.append(person["country"])
    if person.get("continent"):
        location_parts.append(person["continent"])
    
    if location_parts:
        parts.append(f"from {' '.join(location_parts)}")
        
        # Add regional context
        country = (person.get("country") or "").lower()
        if "united states" in country or "america" in country:
            parts.append("American US USA")
        if "united kingdom" in country or "england" in country or "britain" in country:
            parts.append("British English UK")
        if "france" in country:
            parts.append("French European")
        if "germany" in country:
            parts.append("German European")
        if "china" in country:
            parts.append("Chinese Asian")
        if "japan" in country:
            parts.append("Japanese Asian")
        if "india" in country:
            parts.append("Indian Asian")
        if "russia" in country:
            parts.append("Russian")
    
    # 6. Birth/Death years - SANGAT PENTING untuk era context
    if person.get("birth_year"):
        birth_year = person["birth_year"]
        parts.append(f"born {birth_year}")
        
        try:
            year = int(birth_year)
            if year < 1700:
                parts.append("ancient medieval early history classical antiquity")
            elif year < 1800:
                parts.append("18th century colonial era enlightenment founding father revolutionary")
            elif year < 1850:
                parts.append("early 19th century industrial revolution napoleonic era")
            elif year < 1900:
                parts.append("late 19th century victorian era civil war reconstruction")
            elif year < 1920:
                parts.append("early 20th century world war one progressive era")
            elif year < 1945:
                parts.append("interwar period world war two great depression")
            elif year < 1970:
                parts.append("post war cold war civil rights baby boomer")
            elif year < 2000:
                parts.append("late 20th century modern contemporary")
            else:
                parts.append("21st century contemporary modern digital age")
        except:
            pass
    
    if person.get("death_year"):
        parts.append(f"died {person['death_year']}")
    
    # 7. Death info
    if person.get("death_place"):
        parts.append(f"died in {person['death_place']}")
    
    if person.get("cause_of_death"):
        parts.append(f"cause of death {person['cause_of_death']}")
    
    # 8. Description dan Abstract (jika ada - PRIORITAS TINGGI)
    if person.get("description"):
        parts.append(person["description"])
    
    if person.get("abstract"):
        parts.append(person["abstract"])
    
    # 9. Positions (dari relasi)
    if person.get("positions"):
        positions = person["positions"]
        if isinstance(positions, list):
            valid_positions = [p for p in positions if p]
            if valid_positions:
                parts.append("positions: " + ", ".join(valid_positions))
        elif positions:
            parts.append(f"position: {positions}")
    
    # Gabungkan dan clean up
    searchable_text = " ".join(parts)
    searchable_text = " ".join(searchable_text.split())
    
    return searchable_text


def create_searchable_text_event(event: dict) -> str:
    """
    Buat text yang akan di-embed untuk Event.
    
    ⚠️ TIDAK INCLUDE NAMA EVENT - nama di-handle keyword search
    Fokus pada: type, impact, era, location, outcome
    """
    parts = []
    
    # ❌ TIDAK INCLUDE NAMA - nama di-handle keyword search
    # if event.get("name"):
    #     parts.append(event["name"])
    
    # 1. Type of Event (SANGAT PENTING!)
    if event.get("type_of_event"):
        event_type = event["type_of_event"]
        parts.append(event_type)
        
        type_lower = event_type.lower()
        if "war" in type_lower:
            parts.append("military conflict battle combat armed forces warfare violence casualties")
        if "revolution" in type_lower:
            parts.append("uprising rebellion overthrow political change transformation radical")
        if "civil war" in type_lower:
            parts.append("internal conflict domestic strife nation divided brother against brother")
        if "election" in type_lower or "political" in type_lower:
            parts.append("voting democracy government political campaign ballot")
        if "treaty" in type_lower or "agreement" in type_lower or "diplomatic" in type_lower:
            parts.append("negotiation peace deal international relations diplomacy accord")
        if "independence" in type_lower:
            parts.append("freedom liberation sovereignty self-rule colonial separation")
        if "assassination" in type_lower:
            parts.append("murder killing political violence death attack")
        if "disaster" in type_lower or "natural" in type_lower:
            parts.append("catastrophe emergency crisis destruction tragedy")
        if "economic" in type_lower or "financial" in type_lower:
            parts.append("economy market trade business recession depression crash")
        if "reform" in type_lower:
            parts.append("change improvement modernization transformation progress")
        if "protest" in type_lower or "movement" in type_lower:
            parts.append("demonstration activism civil rights social change march")
        if "discovery" in type_lower or "exploration" in type_lower:
            parts.append("scientific breakthrough new finding expedition innovation")
        if "founding" in type_lower or "establishment" in type_lower:
            parts.append("creation beginning start institution organization birth")
        if "coronation" in type_lower or "succession" in type_lower:
            parts.append("monarchy royal king queen throne crown ceremony")
    
    # 2. Year/Era context
    year = event.get("year")
    if year:
        parts.append(f"year {year}")
        try:
            y = int(year)
            if y < 1500:
                parts.append("medieval ancient classical antiquity")
            elif y < 1700:
                parts.append("early modern renaissance reformation colonial")
            elif y < 1800:
                parts.append("18th century enlightenment revolutionary era colonial")
            elif y < 1850:
                parts.append("early 19th century napoleonic industrial revolution")
            elif y < 1900:
                parts.append("late 19th century victorian civil war imperialism")
            elif y < 1920:
                parts.append("early 20th century world war one progressive")
            elif y < 1945:
                parts.append("interwar world war two great depression fascism")
            elif y < 1970:
                parts.append("post war cold war civil rights decolonization")
            elif y < 2000:
                parts.append("late 20th century modern cold war end")
            else:
                parts.append("21st century contemporary modern digital")
        except:
            pass
    
    if event.get("start_date"):
        parts.append(f"started {event['start_date']}")
    if event.get("end_date"):
        parts.append(f"ended {event['end_date']}")
    
    # 3. Location
    if event.get("country"):
        country = event["country"]
        parts.append(f"in {country}")
        
        # Add regional context
        country_lower = country.lower()
        if "united states" in country_lower or "america" in country_lower:
            parts.append("American US USA")
        if "united kingdom" in country_lower or "england" in country_lower:
            parts.append("British English UK")
        if "france" in country_lower:
            parts.append("French European")
        if "germany" in country_lower:
            parts.append("German European")
    
    if event.get("place_name"):
        parts.append(f"at {event['place_name']}")
    
    # 4. Impact (SANGAT PENTING!)
    if event.get("impact"):
        impact = event["impact"]
        parts.append(f"impact: {impact}")
        
        impact_lower = impact.lower()
        if "death" in impact_lower or "killed" in impact_lower or "casualties" in impact_lower:
            parts.append("loss of life fatalities victims tragedy")
        if "independence" in impact_lower or "freedom" in impact_lower:
            parts.append("liberation sovereignty self-determination")
        if "victory" in impact_lower or "won" in impact_lower:
            parts.append("triumph success winning achievement")
        if "defeat" in impact_lower or "lost" in impact_lower:
            parts.append("loss failure surrender")
        if "change" in impact_lower or "transform" in impact_lower:
            parts.append("reform revolution alteration shift")
        if "established" in impact_lower or "created" in impact_lower or "founded" in impact_lower:
            parts.append("beginning creation institution formation")
    
    # 5. Affected Population
    if event.get("affected_population"):
        parts.append(f"affected {event['affected_population']}")
    
    # 6. Important Person/Group
    if event.get("important_person_group"):
        parts.append(f"involving {event['important_person_group']}")
    
    # 7. Outcome
    if event.get("outcome"):
        outcome = event["outcome"]
        parts.append(f"outcome: {outcome}")
        
        outcome_lower = outcome.lower()
        if "success" in outcome_lower or "victory" in outcome_lower:
            parts.append("achievement triumph winning")
        if "failure" in outcome_lower or "defeat" in outcome_lower:
            parts.append("loss unsuccessful")
        if "treaty" in outcome_lower or "peace" in outcome_lower:
            parts.append("agreement resolution end of conflict")
    
    # 8. Description (jika ada)
    if event.get("description"):
        parts.append(event["description"])
    
    # Gabungkan dan clean up
    searchable_text = " ".join(parts)
    searchable_text = " ".join(searchable_text.split())
    
    return searchable_text


def reset_model():
    """Reset model (untuk reload dengan model berbeda)"""
    global _model
    _model = None
    print("🔄 Model reset. Will reload on next use.")