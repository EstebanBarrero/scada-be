import math
from typing import Generic, List, TypeVar

from pydantic import BaseModel, computed_field

T = TypeVar("T")


class Meta(BaseModel):
    page: int
    size: int
    total: int

    @computed_field
    @property
    def pages(self) -> int:
        return math.ceil(self.total / self.size) if self.size > 0 else 0


class PaginatedResponse(BaseModel, Generic[T]):
    """
    Consistent envelope for all paginated list responses.
    Always returns `data` + `meta` — no bare arrays.
    """

    data: List[T]
    meta: Meta
