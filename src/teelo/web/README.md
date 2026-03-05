# Web module boundaries

- `main.py`: app bootstrap only (FastAPI app creation, middleware, static mount, template wiring, and router registration).
- `routers/`: HTTP route ownership by feature area (`public`, `matches`, `rankings`, `players`, `blog`, `admin`).
- `services/`: feature business logic and helper utilities used by routers.
- `schemas/`: request/response DTO definitions.

## Ownership rules

1. Route handlers belong in `routers/*` only.
2. Query/serialization/business helpers belong in `services/*`.
3. `main.py` must not contain endpoint handlers.
4. Shared DTO contracts live in `schemas/*` and should be reused by routers/services when adding new APIs.
