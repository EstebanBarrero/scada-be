from sqlalchemy import Column, Integer, String, Index
from app.database import Base


class Tag(Base):
    """
    Normalized tag dimension table.

    Separating tags from alarms enables:
    - Efficient GROUP BY on tag_id (integer) instead of tag name (string)
    - Central place to enrich tags with area/system metadata
    - Deduplication of tag names across multiple ETL runs
    """

    __tablename__ = "tags"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    area = Column(String(100), nullable=True)
    system = Column(String(100), nullable=True)
    description = Column(String(500), nullable=True)

    __table_args__ = (
        # UNIQUE index on name: prevents duplicate tags, speeds up ETL upsert lookup
        Index("uix_tags_name", "name", unique=True),
    )

    def __repr__(self) -> str:
        return f"<Tag id={self.id} name={self.name!r}>"
