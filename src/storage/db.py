"""
Database Models — Week 4

SQLAlchemy ORM definitions for the PostgreSQL database.
Handles storing derived data: sessions, laps, and setups.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import String, Float, Integer, ForeignKey, Boolean, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class Session(Base):
    __tablename__ = "sessions"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    platform: Mapped[str] = mapped_column(String)
    track: Mapped[str] = mapped_column(String)
    car: Mapped[str] = mapped_column(String)
    session_type: Mapped[str] = mapped_column(String)
    
    start_time: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    end_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    total_laps: Mapped[int] = mapped_column(Integer, default=0)
    best_lap_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    laps: Mapped[list["Lap"]] = relationship("Lap", back_populates="session", cascade="all, delete-orphan")
    setups: Mapped[list["SetupSnapshotDB"]] = relationship("SetupSnapshotDB", back_populates="session", cascade="all, delete-orphan")

class Lap(Base):
    __tablename__ = "laps"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(ForeignKey("sessions.id"))
    lap_number: Mapped[int] = mapped_column(Integer)
    
    lap_time_ms: Mapped[int] = mapped_column(Integer)
    sector_1_ms: Mapped[int] = mapped_column(Integer)
    sector_2_ms: Mapped[int] = mapped_column(Integer)
    sector_3_ms: Mapped[int] = mapped_column(Integer)
    
    is_valid: Mapped[bool] = mapped_column(Boolean)
    fuel_used: Mapped[float] = mapped_column(Float)
    fuel_remaining: Mapped[float] = mapped_column(Float)
    
    # Aggregated averages over the lap
    avg_tyre_temp_fl: Mapped[float] = mapped_column(Float)
    avg_tyre_temp_fr: Mapped[float] = mapped_column(Float)
    avg_tyre_temp_rl: Mapped[float] = mapped_column(Float)
    avg_tyre_temp_rr: Mapped[float] = mapped_column(Float)
    
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    session: Mapped["Session"] = relationship("Session", back_populates="laps")

class SetupSnapshotDB(Base):
    __tablename__ = "setup_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(ForeignKey("sessions.id"))
    lap_number: Mapped[int] = mapped_column(Integer) # The lap this setup was active on
    
    tc_level: Mapped[int] = mapped_column(Integer)
    tc_cut: Mapped[int] = mapped_column(Integer)
    abs_level: Mapped[int] = mapped_column(Integer)
    engine_map: Mapped[int] = mapped_column(Integer)
    brake_bias: Mapped[float] = mapped_column(Float)
    
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    session: Mapped["Session"] = relationship("Session", back_populates="setups")
