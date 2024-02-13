from sqlalchemy import (
    Boolean,
    Column,
    create_engine,
    Date,
    DateTime,
    ForeignKey,
    func,
    Integer,
    String,
    Text,
    Time,
    # Binary,
    LargeBinary,
    UniqueConstraint,
)
from sqlalchemy.dialects.mysql import insert

from sqlalchemy.orm import declarative_base, relationship, scoped_session, sessionmaker


# engine_str = "sqlite:////data/db.sqlite3"
engine_str = "sqlite:///data/db.sqlite3"
engine = create_engine(engine_str)  # echo=True)

session = scoped_session(
    sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
    )
)

Base = declarative_base()


class User(Base):
    __tablename__ = "users_activity"
    action = Column(String(8), nullable=False, primary_key=True)
    # like, reply, repost, quote
    id = Column(String(32), nullable=False, primary_key=True)
    timestamp = Column(DateTime(timezone=False), primary_key=True)
    value = Column(LargeBinary)


class Block(Base):
    __tablename__ = "block_list"
    action = Column(String(8), nullable=False, primary_key=True)
    id = Column(String(32), nullable=False, primary_key=True)
    rkey = Column(String(16), nullable=False)
    timestamp = Column(DateTime(timezone=False))


def initialize_table():
    Base.metadata.create_all(engine)
    session.commit()
