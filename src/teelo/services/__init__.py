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

