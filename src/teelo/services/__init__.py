"""
Teelo services — business logic for data processing pipelines.

This package contains services that orchestrate the data flow:

Pipeline stages:
1. Draw ingestion: Creates scheduled matches from tournament draws
2. Schedule ingestion: Updates matches with schedule details (date, time, court)
3. Results ingestion: Updates matches with final results (score, winner, status)

Usage:
    from teelo.services import (
        ingest_draw,
        ingest_schedule,
        ingest_results,
    )
"""

from teelo.services.draw_ingestion import (
    ingest_draw,
    propagate_draw_result,
    DrawIngestionStats,
)
from teelo.services.schedule_ingestion import (
    ingest_schedule,
    ingest_single_fixture,
    ScheduleIngestionStats,
)
from teelo.services.results_ingestion import (
    ingest_results,
    ingest_single_result,
    ResultsIngestionStats,
)

__all__ = [
    # Draw ingestion
    "ingest_draw",
    "propagate_draw_result",
    "DrawIngestionStats",
    # Schedule ingestion
    "ingest_schedule",
    "ingest_single_fixture",
    "ScheduleIngestionStats",
    # Results ingestion
    "ingest_results",
    "ingest_single_result",
    "ResultsIngestionStats",
]
