from pydantic import BaseModel

class CypherQueryRequest(BaseModel):
    query: str