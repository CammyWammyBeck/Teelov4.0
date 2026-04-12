# Stripe Donations Code Structure Plan

Status: Planning only — no implementation yet.
Related: `docs/plans/2026-04-11-stripe-donations-implementation-plan.md`
Date: 2026-04-11
Owner: Cam / Barry

## Purpose

This document answers the engineering-structure question:

- what code should be added
- where it should live
- which module owns which responsibility
- how data should flow through the app
- how implementation should be sequenced

This is deliberately detailed so the eventual build has minimal ambiguity.

---

## Existing Teelo web boundaries

From `src/teelo/web/README.md`:

- `main.py` = app bootstrap only
- `routers/` = HTTP route ownership by feature area
- `services/` = business logic / helpers
- `schemas/` = request / response DTOs

This donations feature should follow those boundaries cleanly.

---

## Recommended feature shape

Stripe donations should be implemented as a **public web feature** with a small service layer and one dedicated schema module.

### Core design principle

- **Routers** own HTTP endpoints and request/response wiring
- **Services** own Stripe interaction, donation persistence, validation helpers, and webhook processing
- **Schemas** define DTOs for create-intent and status responses
- **Templates** own rendered HTML
- **Static JS** owns amount selection, countdown, and Stripe Elements client behaviour
- **DB models** own persistence shape

Do not bury payment logic inside template handlers or make routers directly talk to Stripe.

---

## Recommended files to add or modify

## 1) Configuration

### Modify
- `src/teelo/config.py`

### Add settings
- `stripe_publishable_key: Optional[str]`
- `stripe_secret_key: Optional[str]`
- `stripe_webhook_secret: Optional[str]`
- `stripe_currency_default: str = "aud"`
- `stripe_donations_enabled: bool = False`
- `stripe_custom_amount_min_cents: int = 500`
- `stripe_custom_amount_max_cents: int = 100000`
- `site_base_url: Optional[str]`

### Responsibility
This remains the single source of config truth. No Stripe constants should be scattered through services/templates.

---

## 2) Database models

### Modify
- `src/teelo/db/models.py`

### Add model: `Donation`

Recommended placement:
- after existing admin/ops-ish models, or in a clearly labelled new section such as:
  - `# Support / Payments Models`

### Add model: `StripeWebhookEvent`

Recommended placement:
- directly after `Donation`

### Why keep both in `models.py`
The codebase currently keeps ORM models centralised in one file. Follow that pattern rather than introducing a new model module just for this feature.

---

## 3) Migration

### Add
- `alembic/versions/YYYYMMDD_HHMMSS_add_donations_tables.py`

### Responsibility
- create `donations`
- create `stripe_webhook_events`
- add indexes / unique constraints

Migration should be self-contained and not sneak in unrelated schema changes.

---

## 4) Web schemas

### Add
- `src/teelo/web/schemas/donation.py`

### Purpose
Keep request and response contracts explicit.

### Recommended DTOs

#### `CreateDonationIntentRequest`
Fields:
- `amount_cents: int`
- `currency: str`
- `donor_name: str | None`
- `donor_email: str | None`
- `donor_message: str | None`
- `is_anonymous: bool = False`

#### `CreateDonationIntentResponse`
Fields:
- `public_id: str`
- `client_secret: str`
- `status: str`

#### `DonationStatusResponse`
Fields:
- `public_id: str`
- `status: str`

### Why schemas matter here
Payment APIs get messy fast if contracts are implicit. Tight DTOs reduce accidental drift between frontend and backend.

---

## 5) Public router

### Modify
- `src/teelo/web/routers/public.py`

### Reason
This feature is public-facing, not admin-only, and belongs with other public site routes.

### Recommended route ownership

Public page routes:
- `GET /donate`
- `GET /donate/success`
- `GET /donate/error`

API routes:
- `POST /api/donations/create-payment-intent`
- `GET /api/donations/{public_id}/status`
- `POST /api/stripe/webhook`

### Router responsibility
The router should:
- parse request bodies
- call service functions
- return template responses or JSON responses
- not contain business logic beyond thin request handling

---

## 6) Donation service layer

### Add
- `src/teelo/web/services/donation_service.py`

This should become the main business-logic home for the feature.

### Recommended functions

#### Page/data helpers
- `build_donate_page_context()`
- `build_donation_success_context(public_id: str | None)`
- `build_donation_error_context(...)`

#### Validation / normalisation
- `normalize_optional_text(value: str | None) -> str | None`
- `normalize_email(value: str | None) -> str | None`
- `validate_donation_amount(amount_cents: int, currency: str) -> None`
- `validate_donation_fields(...) -> None`

#### Donation persistence / Stripe intent flow
- `create_donation_and_payment_intent(payload: CreateDonationIntentRequest) -> CreateDonationIntentResponse`
- `get_donation_status(public_id: str) -> DonationStatusResponse`

#### Webhook processing
- `record_webhook_event(...)`
- `process_stripe_webhook(raw_body: bytes, signature: str | None) -> None`
- `_handle_payment_intent_succeeded(...)`
- `_handle_payment_intent_failed(...)`
- `_handle_charge_refunded(...)`

### Why a dedicated donation_service
This feature has enough moving parts to justify its own service module. Do not cram this into `legacy_main_handlers.py` or an unrelated general-purpose helper.

---

## 7) Optional Stripe helper module

### Optional add
- `src/teelo/services/stripe_support.py`
  or
- `src/teelo/web/services/stripe_client.py`

### When to do this
If `donation_service.py` starts getting too mixed between app logic and Stripe SDK calls, extract Stripe-specific calls into a narrow helper.

### Recommended responsibility if added
- create Payment Intent
- parse/verify webhook event
- map Stripe object fields into Teelo-safe structures

### My recommendation
Start with **one `donation_service.py` module** unless it becomes obviously too large. Don’t over-abstract on day one.

---

## 8) Templates

### Add
- `src/teelo/web/templates/donate.html`
- `src/teelo/web/templates/donate_success.html`
- `src/teelo/web/templates/donate_error.html`

### Template ownership

#### `donate.html`
Owns:
- Support Teelo copy
- preset amount buttons
- custom amount field
- optional name/email/message inputs
- anonymous checkbox
- legal/support framing note
- Stripe Elements mount point
- error/status area
- JS bootstrapping data

#### `donate_success.html`
Owns:
- generic thank-you message
- safe, non-sensitive success state
- optional “still confirming” messaging if webhook state lags briefly

#### `donate_error.html`
Owns:
- failure / incomplete payment message
- retry CTA back to donation page

### Template design principle
Keep templates mostly declarative. Payment state logic should live in JS and services, not in giant inline scripts.

---

## 9) Static JS

### Add
- `src/teelo/web/static/js/donate.js`

### Recommended responsibility
This file should own the client-side donation experience.

### Recommended client responsibilities

#### UI state
- preset amount selection
- custom amount input behaviour
- optional field collection
- anonymous checkbox state
- submit button disabled/loading state
- inline error display

#### Message countdown
- 240 char max
- countdown appears only near the limit
- recommended threshold: last 50 characters

#### Stripe behaviour
- initialise Stripe with publishable key
- mount Payment Element
- call create-payment-intent endpoint
- confirm payment using Stripe.js
- navigate to success/error state

### Avoid
- embedding large payment logic inline in the template
- duplicating validation rules that must remain server-side authoritative

---

## 10) Static CSS / styling

### Likely no new dedicated CSS file needed
Use existing Tailwind/CSS pipeline and existing utility classes.

### Modify if needed
- `src/teelo/web/static/css/input.css`
- rebuilt output/styles file if the project currently regenerates CSS that way

### Guideline
Prefer existing design system classes rather than one-off bespoke styling.

---

## 11) Tests

## Backend tests

### Add likely files
- `tests/unit/test_donation_service.py`
- `tests/integration/test_donation_routes.py`
- optionally `tests/integration/test_stripe_webhooks.py`

### Unit coverage should include
- amount validation
- min/max enforcement
- field normalisation
- email validation
- message limit enforcement
- anonymous flag handling
- webhook idempotency
- status transitions

### Integration coverage should include
- public page rendering
- create-payment-intent contract
- status endpoint contract
- webhook processing flow
- duplicate webhook event behaviour

## Frontend JS tests

Optional depending on existing practice, but if the current JS test setup is active:
- add targeted tests for amount selection helpers and countdown logic

---

## Detailed module responsibilities

## `public.py` router

### Should do
- define HTTP routes
- call donation service
- return templates or JSON

### Should not do
- talk to Stripe directly
- manually implement field validation rules inline
- perform webhook business logic inline

## `donation_service.py`

### Should do
- central business rules
- Stripe SDK calls or coordination
- DB read/write operations for donation feature
- state transitions
- webhook mapping logic
- page context building if helpful

### Should not do
- render templates directly
- define FastAPI routes
- own large chunks of HTML/JS formatting

## `schemas/donation.py`

### Should do
- request validation shape
- response shape

### Should not do
- business logic or DB access

## `donate.js`

### Should do
- client UX only
- Stripe.js interaction
- lightweight client-side validation for usability

### Should not do
- assume client validation is authoritative
- contain hardcoded secret values

---

## Proposed code flow by endpoint

## 1) `GET /donate`

### Router
`public.py`

### Service call
`build_donate_page_context()`

### Template
`donate.html`

### Data passed to template
- Stripe publishable key
- currency = AUD
- preset amounts
- custom amount min/max
- message max length
- legal/support copy
- donations feature flag state

---

## 2) `POST /api/donations/create-payment-intent`

### Router
`public.py`

### Input schema
`CreateDonationIntentRequest`

### Service
`create_donation_and_payment_intent()`

### Service internal steps
1. normalise payload
2. validate amount and optional fields
3. insert donation row with `created`
4. call Stripe to create Payment Intent
5. update donation row with Stripe IDs and `pending`
6. return `public_id + client_secret + status`

### Notes
Use a transaction pattern that leaves the DB in a recoverable state even if Stripe creation fails mid-process.

---

## 3) `GET /api/donations/{public_id}/status`

### Router
`public.py`

### Service
`get_donation_status(public_id)`

### Behaviour
- return current server-known state
- do not leak donor PII
- 404 if reference not found

---

## 4) `POST /api/stripe/webhook`

### Router
`public.py`

### Service
`process_stripe_webhook(raw_body, signature)`

### Internal webhook steps
1. verify signature
2. parse event
3. upsert/log `stripe_webhook_events`
4. if already processed, return safely
5. map Stripe intent/charge back to `Donation`
6. apply state transition idempotently
7. mark webhook event processed

### Canonical mappings
- `payment_intent.succeeded` -> `paid`
- `payment_intent.payment_failed` -> `failed`
- `charge.refunded` -> `refunded`

---

## DB model notes

## `Donation`

### Suggested SQLAlchemy fields
- `id: int`
- `public_id: str`
- `status: str`
- `currency: str`
- `amount_cents: int`
- `is_custom_amount: bool`
- `donor_name: Optional[str]`
- `donor_email: Optional[str]`
- `donor_message: Optional[str]`
- `is_anonymous: bool`
- `stripe_payment_intent_id: Optional[str]`
- `stripe_customer_id: Optional[str]`
- `stripe_latest_charge_id: Optional[str]`
- `stripe_last_event_id: Optional[str]`
- `payment_method_type: Optional[str]`
- `paid_at: Optional[datetime]`
- `failed_at: Optional[datetime]`
- `refunded_at: Optional[datetime]`
- `created_at: datetime`
- `updated_at: datetime`

## `StripeWebhookEvent`

### Suggested SQLAlchemy fields
- `id: int`
- `stripe_event_id: str`
- `event_type: str`
- `object_id: Optional[str]`
- `processing_status: str`
- `related_donation_id: Optional[int]`
- `error_text: Optional[str]`
- `received_at: datetime`
- `processed_at: Optional[datetime]`

---

## Error handling strategy

## Create Payment Intent failures

Handle separately:
- validation failure -> 4xx JSON with field-safe message
- Stripe API failure -> 5xx or controlled failure JSON with generic payment error
- DB failure -> generic server error

### Important
Do not expose raw Stripe exception detail directly to the browser.

## Webhook failures

- invalid signature -> reject immediately
- transient processing error -> log and fail cleanly so Stripe can retry
- already processed -> return success safely

---

## Data ownership and privacy notes

### Store
- amount
- currency
- optional donor fields
- anonymous flag
- Stripe identifiers needed for reconciliation

### Do not store
- raw card details
- unnecessary webhook payload blobs unless a strong reason emerges

### Anonymous note
Because there is no public donor wall, anonymity is mainly about how Teelo treats donor identity in UI/future display. Keep the flag, even if it has limited visible effect initially.

---

## Implementation order

This is the order I’d use once coding is approved.

### Step 1 — Config + models + migration
- add Stripe settings
- add `Donation` and `StripeWebhookEvent`
- create migration

### Step 2 — Schemas + service core
- add `schemas/donation.py`
- build validation/normalisation helpers
- build create-intent flow
- build status lookup

### Step 3 — Router wiring
- add public routes and API endpoints
- wire service calls

### Step 4 — Templates + JS
- build `donate.html`
- build success/error pages
- build `donate.js`
- wire Stripe Elements client flow

### Step 5 — Webhook processing
- add verified webhook endpoint
- idempotent event logging and donation state transitions

### Step 6 — Tests
- unit tests
- integration tests
- manual Stripe test-mode pass

---

## Final architectural recommendation

Do **not** implement this as a scattered set of one-off changes across random files.

The clean structure is:

- config in `config.py`
- persistence in `db/models.py` + migration
- HTTP ownership in `web/routers/public.py`
- business/payment logic in `web/services/donation_service.py`
- DTO contracts in `web/schemas/donation.py`
- rendered UI in `web/templates/donate*.html`
- client payment UX in `web/static/js/donate.js`

That keeps the feature legible, testable, and maintainable — and means when you come back to it in six months, it won’t feel like payment spaghetti.
