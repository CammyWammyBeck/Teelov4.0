# Teelo v4.0 Architecture Plan

## Overview

This document covers the **remaining planned phases** for Teelo v4.0. For current project state and completed work, see `CLAUDE.md`.

**Completed:** Phase 1 (Foundation) - database schema, migrations, player identity, ATP scraper, ELO calculator, score parser, temporal ordering.

**Next:** Phase 2 (ELO & Ratings) → Phase 3 (Website) → Phase 4 (Expand Coverage) → Phase 5 (ML & Predictions). See CLAUDE.md for detailed status.

**Project focus:** The priority for v4.0 is building a beautiful public website that makes tennis data accessible. ML predictions come later.

---

## Phase 3: Website

### FastAPI Endpoints

```python
# src/teelo/api/main.py

app = FastAPI(title="Teelo Tennis API", version="4.0.0")

# Routers:
# /api/players    - Player search, details, H2H
# /api/matches    - Match history, filtering
# /api/rankings   - ELO rankings, history
# /api/tournaments - Tournament data
# /api/admin      - Review queue management
```

### Admin Endpoints

- `GET /api/admin/review-queue` - Pending player matches with suggestions
- `POST /api/admin/review-queue/{id}/resolve` - Match, create, or ignore

### Web Frontend

Technology TBD - likely Next.js or similar. Key pages:
- ELO rankings (live, filterable by tour/surface)
- Player profiles with ELO history charts
- Match history and tournament results
- Head-to-head comparisons
- Tournament pages

---

## Phase 5: Feature Store & ML Pipeline

### 3.1 Feature Store Architecture

The feature store allows easy experimentation with new features.

```python
# src/teelo/features/store.py

class FeatureStore:
    """
    Manages feature definitions and computed features.
    Features are versioned - when you change a feature's logic,
    you create a new version and can compare model performance.
    """

    def register_feature(self, feature_class: Type[BaseFeature]):
        """Register a feature class."""

    def create_feature_set(self, name, version, feature_names, description="") -> FeatureSet:
        """Create a new feature set combining multiple features."""

    def compute_features(self, match_id, feature_set_id) -> dict:
        """Compute all features for a match."""
```

Feature definitions as modular classes:

```python
# src/teelo/features/definitions/base.py

class BaseFeature(ABC):
    name: str
    version: str

    @abstractmethod
    def compute(self, match_id: int) -> dict[str, Any]:
        """Compute feature values for a match."""

    @abstractmethod
    def schema(self) -> dict:
        """Return schema describing feature outputs."""
```

Planned feature modules:
- `elo.py` - ELO-based features (current rating, peak, difference, vs-peak)
- `h2h.py` - Head-to-head record features
- `form.py` - Recent form features (last N matches)
- `surface.py` - Surface-specific performance stats
- `stats.py` - Match stats embeddings

### 3.2 Model Registry

```python
# src/teelo/ml/registry.py

class ModelRegistry:
    """
    Manages trained models with versioning.
    Each model version includes: trained model, scaler,
    feature set definition, training metrics.
    """

    def save_model(self, model, scaler, feature_set_name, metrics, calibrator=None) -> str:
    def load_model(self, version="latest"):
    def list_models(self) -> list[dict]:
```

### 3.3 Continuous Learning System

```
Match Data → Feature Store → Training Queue → Model Registry
                                                    ↓
              Model Performance Monitor ←───────────┘
                        ↓
              Retrain Trigger (drift/threshold)
```

Key components:
- **ModelPerformanceMonitor** - Tracks accuracy, Brier score, calibration (rolling 7d/30d)
- **Retrain triggers**: 30d accuracy < 62%, calibration error > 0.05, accuracy drift > 3%, 500+ new matches
- **TrainingPipeline** - Automated data prep → train → validate → deploy (only if improvement)
- **FeatureDriftDetector** - KS test on feature distributions, weekly checks
- **ABTestManager** - Deterministic assignment by fixture ID, statistical significance testing

---

## Deployment & Operations

### 5.1 Infrastructure

- **Database**: Cloud-hosted PostgreSQL (accessible from all machines and API)
- **API**: Deployable to cloud host (Heroku, Railway, etc.)
- **Scrapers**: Run on Arch Linux server via scheduled tasks
- **Docker**: Optional local dev via docker-compose

### 5.2 Scheduled Tasks

```python
# Hourly: scrape results, fixtures, calculate ELO, generate predictions
# Every 2 hours: scrape betting odds
# Daily 4am: refresh materialized views
# Daily 6am: check model performance
# Weekly Monday: check feature drift
# Weekly Sunday 3am: conditional retrain
```

### 5.3 Discord Integration

- System alerts (scraping failures, model drift, retrain triggers)
- Betting opportunity alerts (expected value > 5% edge)
- Daily review queue digest

---

## Phase 6: Testing Strategy

1. **Unit tests** - ELO calculator, score parser, feature computations
2. **Integration tests** - API endpoints, database operations
3. **Scraper validation** - Check parsed data matches expected format
4. **ML tests** - Feature computation consistency, prediction pipeline

---

## Implementation Roadmap

| Phase | Focus | Status |
|-------|-------|--------|
| Phase 1: Foundation | DB schema, player identity, ATP scraper, ELO | COMPLETE |
| Phase 2: ELO & Ratings | Historical scraping, ELO pipeline, cloud DB | NEXT |
| Phase 3: Website | FastAPI endpoints, web frontend, data browsing | PLANNED |
| Phase 4: Expand Coverage | WTA/ITF scrapers, betting odds, automation | PLANNED |
| Phase 5: ML & Predictions | Feature store, model registry, continuous learning | PLANNED |

---

## Key Improvements Over v3.0

| Issue in v3.0 | Solution in v4.0 |
|--------------|------------------|
| Fragile name matching | Canonical player IDs with alias table |
| N+1 database queries | Feature store with pre-computed features |
| Silent failures | Queue-based retry with error tracking |
| Monolithic functions | Modular feature definitions |
| Single betting source | Extensible betting scraper base class |
| No testing | Comprehensive test suite |
| Mixed async/sync | Consistent async throughout |
| Hardcoded constants | Configuration via Pydantic settings |
| Single model version | Model registry with versioning |
| Manual deployment | Docker + scheduled tasks |
