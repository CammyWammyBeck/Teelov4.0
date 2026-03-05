from pydantic import BaseModel


class PlayerDTO(BaseModel):
    id: int
    name: str
