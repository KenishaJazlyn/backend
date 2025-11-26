from pydantic import BaseModel

class EnrichName(BaseModel):
    name: str

class EnrichConfirm(BaseModel):
    name: str
    qid: str

class EnrichNamesList(BaseModel):
    names: list[str]