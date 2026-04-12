# Stripe Donations Function-by-Function Breakdown

Status: Planning only — no implementation yet.
Related:
- `docs/plans/2026-04-11-stripe-donations-implementation-plan.md`
- `docs/plans/2026-04-11-stripe-donations-code-structure.md`
Date: 2026-04-11
Owner: Cam / Barry

## Purpose

This document turns the donations plan into an implementation checklist at the function and file level.

The goal is to remove ambiguity before coding starts:
- exact files
- expected functions/classes
- suggested signatures
- each function’s job
- order of implementation
- known edge cases

This is still planning only.

---

## 1) `src/teelo/config.py`

## Fields to add to `Settings`

### `stripe_publishable_key`
```python
stripe_publishable_key: Optional[str] = Field(default=None)
```
Purpose:
- used by the donate page frontend to initialise Stripe.js

### `stripe_secret_key`
```python
stripe_secret_key: Optional[str] = Field(default=None)
```
Purpose:
- used server-side to create Payment Intents and verify Stripe objects

### `stripe_webhook_secret`
```python
stripe_webhook_secret: Optional[str] = Field(default=None)
```
Purpose:
- used to verify webhook signatures

### `stripe_currency_default`
```python
stripe_currency_default: str = Field(default="aud")
```
Purpose:
- canonical server-side currency

### `stripe_donations_enabled`
```python
stripe_donations_enabled: bool = Field(default=False)
```
Purpose:
- feature flag for safe rollout

### `stripe_custom_amount_min_cents`
```python
stripe_custom_amount_min_cents: int = Field(default=500)
```
Purpose:
- A$5 minimum

### `stripe_custom_amount_max_cents`
```python
stripe_custom_amount_max_cents: int = Field(default=100000)
```
Purpose:
- A$1,000 maximum

### `site_base_url`
```python
site_base_url: Optional[str] = Field(default=None)
```
Purpose:
- success/error URL construction if needed

## Optional validator

### `validate_stripe_currency_default`
Suggested signature:
```python
@field_validator("stripe_currency_default")
@classmethod
def validate_stripe_currency_default(cls, v: str) -> str:
    ...
```
Responsibility:
- normalise to lowercase
- optionally restrict to `aud` for this feature

---

## 2) `src/teelo/db/models.py`

## Class: `Donation`

Suggested model shape:

```python
class Donation(Base):
    __tablename__ = "donations"
    ...
```

### Fields
- `id`
- `public_id`
- `status`
- `currency`
- `amount_cents`
- `is_custom_amount`
- `donor_name`
- `donor_email`
- `donor_message`
- `is_anonymous`
- `stripe_payment_intent_id`
- `stripe_customer_id`
- `stripe_latest_charge_id`
- `stripe_last_event_id`
- `payment_method_type`
- `paid_at`
- `failed_at`
- `refunded_at`
- `created_at`
- `updated_at`

### Optional helper properties / methods

#### `is_paid`
```python
@property
def is_paid(self) -> bool:
    return self.status == "paid"
```

#### `is_terminal`
```python
@property
def is_terminal(self) -> bool:
    return self.status in ("paid", "failed", "cancelled", "refunded")
```

These aren’t essential, but can make service logic cleaner.

## Class: `StripeWebhookEvent`

Suggested shape:

```python
class StripeWebhookEvent(Base):
    __tablename__ = "stripe_webhook_events"
    ...
```

### Fields
- `id`
- `stripe_event_id`
- `event_type`
- `object_id`
- `processing_status`
- `related_donation_id`
- `error_text`
- `received_at`
- `processed_at`

---

## 3) `alembic/versions/...add_donations_tables.py`

## `upgrade()`
Responsibilities:
- create `donations`
- create `stripe_webhook_events`
- create indexes and unique constraints

## `downgrade()`
Responsibilities:
- drop `stripe_webhook_events`
- drop `donations`

### Constraint/index notes

For `donations`:
- unique `public_id`
- unique nullable `stripe_payment_intent_id`
- index `status`
- index `created_at`
- index `paid_at`

For `stripe_webhook_events`:
- unique `stripe_event_id`
- index `event_type`
- index `processing_status`
- index `received_at`

---

## 4) `src/teelo/web/schemas/donation.py`

This module should define the API contract.

## Class: `CreateDonationIntentRequest`

Suggested fields:
```python
class CreateDonationIntentRequest(BaseModel):
    amount_cents: int
    currency: str
    donor_name: str | None = None
    donor_email: str | None = None
    donor_message: str | None = None
    is_anonymous: bool = False
```

### Optional validators

#### `validate_currency`
- ensure lowercase handling or exact `aud`
- still enforce final truth server-side in service

#### `validate_message_length`
- keep schema-level guard at 240 max
- service should still enforce too

## Class: `CreateDonationIntentResponse`

```python
class CreateDonationIntentResponse(BaseModel):
    public_id: str
    client_secret: str
    status: str
```

## Class: `DonationStatusResponse`

```python
class DonationStatusResponse(BaseModel):
    public_id: str
    status: str
```

---

## 5) `src/teelo/web/services/donation_service.py`

This is the core implementation module.

## Constants to define

### `DONATION_PRESET_AMOUNTS_CENTS`
```python
DONATION_PRESET_AMOUNTS_CENTS = (500, 1000, 2500, 5000)
```

### `DONATION_MESSAGE_MAX_LENGTH`
```python
DONATION_MESSAGE_MAX_LENGTH = 240
```

### `DONATION_NAME_MAX_LENGTH`
```python
DONATION_NAME_MAX_LENGTH = 80
```

### `DONATION_STATUS_*`
Optional constants for:
- `created`
- `pending`
- `paid`
- `failed`
- `cancelled`
- `refunded`

This reduces typo risk.

---

## Helper function: `normalize_optional_text`
Suggested signature:
```python
def normalize_optional_text(value: str | None) -> str | None:
    ...
```
Responsibilities:
- convert empty strings / whitespace-only to `None`
- trim leading/trailing whitespace
- preserve internal content

## Helper function: `normalize_email`
Suggested signature:
```python
def normalize_email(value: str | None) -> str | None:
    ...
```
Responsibilities:
- trim whitespace
- lowercase
- convert empty to `None`
- not perform deep deliverability validation

## Helper function: `is_custom_amount`
Suggested signature:
```python
def is_custom_amount(amount_cents: int) -> bool:
    ...
```
Responsibilities:
- return whether amount differs from preset set

## Helper function: `validate_donation_amount`
Suggested signature:
```python
def validate_donation_amount(amount_cents: int, currency: str) -> None:
    ...
```
Responsibilities:
- assert currency is `aud`
- assert amount is integer-like and positive
- assert amount is between configured min/max
- optionally ensure cents granularity matches intended UX if necessary
- raise controlled validation exception on failure

## Helper function: `validate_donation_fields`
Suggested signature:
```python
def validate_donation_fields(
    donor_name: str | None,
    donor_email: str | None,
    donor_message: str | None,
) -> None:
    ...
```
Responsibilities:
- enforce name <= 80
- enforce message <= 240
- enforce email format if provided
- raise controlled validation exception on failure

## Helper function: `build_stripe_metadata`
Suggested signature:
```python
def build_stripe_metadata(donation: Donation) -> dict[str, str]:
    ...
```
Responsibilities:
- create minimal safe metadata payload for Stripe
- include `public_id`
- include one-time marker if helpful
- avoid sensitive note/email stuffing

## Helper function: `create_public_id`
Suggested signature:
```python
def create_public_id() -> str:
    ...
```
Responsibilities:
- generate stable external-safe identifier
- short enough for URLs, unpredictable enough to not guess

Recommended implementation style:
- UUID-based or token_urlsafe-derived
- do not use sequential IDs

---

## Page-context function: `build_donate_page_context`
Suggested signature:
```python
def build_donate_page_context() -> dict:
    ...
```
Responsibilities:
- return template context for `/donate`
- include publishable key
- include preset amounts
- include min/max custom amount
- include message max length
- include legal/support copy
- include feature flag state

## Page-context function: `build_donation_success_context`
Suggested signature:
```python
def build_donation_success_context(public_id: str | None) -> dict:
    ...
```
Responsibilities:
- produce safe generic page context
- optionally include donation ref for polling/status lookup
- avoid including donor PII

## Page-context function: `build_donation_error_context`
Suggested signature:
```python
def build_donation_error_context() -> dict:
    ...
```
Responsibilities:
- produce safe generic retry context

---

## Core function: `create_donation_and_payment_intent`
Suggested signature:
```python
def create_donation_and_payment_intent(
    payload: CreateDonationIntentRequest,
) -> CreateDonationIntentResponse:
    ...
```

### Internal step order

1. verify donations feature flag is enabled
2. normalise `donor_name`, `donor_email`, `donor_message`
3. validate amount
4. validate optional fields
5. create `Donation` row with:
   - `public_id`
   - `status = created`
   - amount/currency
   - donor fields
   - anonymous flag
   - `is_custom_amount`
6. flush/commit enough to get DB identity if needed
7. call Stripe Payment Intent creation
8. update donation row with:
   - `stripe_payment_intent_id`
   - maybe `stripe_customer_id`
   - `status = pending`
9. commit
10. return response DTO

### Failure handling

If Stripe creation fails after donation row is inserted:
- either leave donation in `created`/`failed` state with clear recoverability
- or wrap in a transaction strategy that records failure explicitly

My recommendation:
- create row first for traceability
- if Stripe creation fails, set `status = failed` and record enough context for logs

---

## Core function: `get_donation_status`
Suggested signature:
```python
def get_donation_status(public_id: str) -> DonationStatusResponse:
    ...
```
Responsibilities:
- fetch donation by `public_id`
- return `public_id + status`
- raise not-found exception if absent
- never expose donor data

---

## Core function: `verify_and_construct_stripe_event`
Suggested signature:
```python
def verify_and_construct_stripe_event(raw_body: bytes, signature: str | None):
    ...
```
Responsibilities:
- wrap Stripe SDK webhook verification
- return parsed event object
- raise controlled auth/verification exception on failure

This function isolates the Stripe-specific webhook verification logic.

## Core function: `record_webhook_received`
Suggested signature:
```python
def record_webhook_received(event_id: str, event_type: str, object_id: str | None) -> StripeWebhookEvent:
    ...
```
Responsibilities:
- persist receipt of webhook
- enforce unique event handling
- if already exists, caller can decide whether to no-op or inspect status

## Core function: `process_stripe_webhook`
Suggested signature:
```python
def process_stripe_webhook(raw_body: bytes, signature: str | None) -> None:
    ...
```

### Internal step order

1. verify and parse event
2. inspect event type + object id
3. persist `stripe_webhook_events` receipt row if new
4. if event already processed, exit safely
5. branch to event-specific handler
6. mark webhook row processed / ignored / failed
7. commit transaction

### Supported event types
- `payment_intent.succeeded`
- `payment_intent.payment_failed`
- `charge.refunded`

---

## Event handler: `_handle_payment_intent_succeeded`
Suggested signature:
```python
def _handle_payment_intent_succeeded(event, webhook_row: StripeWebhookEvent) -> None:
    ...
```
Responsibilities:
- extract Payment Intent ID
- find matching `Donation`
- no-op safely if already `paid`
- update donation fields:
  - `status = paid`
  - `paid_at`
  - `stripe_last_event_id`
  - `stripe_latest_charge_id` if available
  - `payment_method_type` if available
- relate webhook row to donation

## Event handler: `_handle_payment_intent_payment_failed`
Suggested signature:
```python
def _handle_payment_intent_payment_failed(event, webhook_row: StripeWebhookEvent) -> None:
    ...
```
Responsibilities:
- map Payment Intent failure to donation
- no-op safely if already terminal in a conflicting-successful way
- set `failed` state only if appropriate
- update `failed_at`
- record last event ID

## Event handler: `_handle_charge_refunded`
Suggested signature:
```python
def _handle_charge_refunded(event, webhook_row: StripeWebhookEvent) -> None:
    ...
```
Responsibilities:
- map refunded charge back to donation via stored charge/payment intent IDs
- set `refunded`
- set `refunded_at`
- no-op safely if already refunded

---

## Logging helper: `log_donation_event`
Optional helper:
```python
def log_donation_event(event_name: str, **kwargs) -> None:
    ...
```
Purpose:
- standardise structured logging fields

This is optional, but useful if you want cleaner logs.

---

## 6) `src/teelo/web/routers/public.py`

The route handlers should stay thin.

## Route: `donate_page`
Suggested signature:
```python
@router.get("/donate", response_class=HTMLResponse)
async def donate_page(request: Request):
    ...
```
Responsibilities:
- call `build_donate_page_context()`
- render `donate.html`

## Route: `donate_success_page`
Suggested signature:
```python
@router.get("/donate/success", response_class=HTMLResponse)
async def donate_success_page(request: Request, ref: str | None = None):
    ...
```
Responsibilities:
- call `build_donation_success_context(ref)`
- render `donate_success.html`

## Route: `donate_error_page`
Suggested signature:
```python
@router.get("/donate/error", response_class=HTMLResponse)
async def donate_error_page(request: Request):
    ...
```
Responsibilities:
- call `build_donation_error_context()`
- render `donate_error.html`

## Route: `create_donation_payment_intent`
Suggested signature:
```python
@router.post("/api/donations/create-payment-intent")
async def create_donation_payment_intent(payload: CreateDonationIntentRequest):
    ...
```
Responsibilities:
- pass schema object to service
- return response DTO / JSON
- map service exceptions to HTTP status codes cleanly

## Route: `donation_status`
Suggested signature:
```python
@router.get("/api/donations/{public_id}/status")
async def donation_status(public_id: str):
    ...
```
Responsibilities:
- return `DonationStatusResponse`
- 404 if not found

## Route: `stripe_webhook`
Suggested signature:
```python
@router.post("/api/stripe/webhook")
async def stripe_webhook(request: Request):
    ...
```
Responsibilities:
- read raw body
- read Stripe signature header
- call `process_stripe_webhook`
- return 200 quickly on success
- return proper failure on invalid signature/processing error

---

## 7) Templates

## `donate.html`

### Expected blocks
- headline / support copy
- legal framing copy
- preset amount buttons
- custom amount input
- optional name input
- optional email input
- optional message textarea
- anonymous checkbox
- message countdown element
- Stripe payment element mount container
- inline error box
- submit button
- JSON/bootstrap data attributes for JS

### Data expected from backend
- `stripe_publishable_key`
- `currency`
- `preset_amounts`
- `custom_min_cents`
- `custom_max_cents`
- `message_max_length`
- `legal_copy_short`
- `support_enabled`

## `donate_success.html`

### Expected blocks
- generic thank-you message
- optional “confirming payment” note
- optional JS hook for status polling if ref present
- button back to home / relevant area

## `donate_error.html`

### Expected blocks
- generic failure message
- retry button to `/donate`
- optional brief reassurance if payment may simply not have completed

---

## 8) `src/teelo/web/static/js/donate.js`

This file should be organised into small functions, not one giant script blob.

## Recommended constants

```javascript
const MESSAGE_COUNTDOWN_THRESHOLD = 50;
const MESSAGE_MAX_LENGTH = 240;
```

## Function: `getSelectedAmountCents`
Suggested signature:
```javascript
function getSelectedAmountCents() { ... }
```
Responsibilities:
- determine active preset or custom amount
- return integer cents or validation failure state

## Function: `setPresetAmount`
```javascript
function setPresetAmount(amountCents) { ... }
```
Responsibilities:
- visually activate preset
- clear/override custom amount UX state

## Function: `clearPresetSelection`
```javascript
function clearPresetSelection() { ... }
```
Responsibilities:
- deselect preset buttons when custom input becomes authoritative

## Function: `updateMessageCountdown`
```javascript
function updateMessageCountdown() { ... }
```
Responsibilities:
- compute remaining chars
- show countdown when remaining <= threshold
- style warning state if at/over limit

## Function: `collectFormPayload`
```javascript
function collectFormPayload() { ... }
```
Responsibilities:
- read amount + optional fields + anonymous flag
- return payload for backend

## Function: `setSubmittingState`
```javascript
function setSubmittingState(isSubmitting) { ... }
```
Responsibilities:
- disable/enable button
- toggle spinner/text
- reduce duplicate submissions

## Function: `showInlineError`
```javascript
function showInlineError(message) { ... }
```
Responsibilities:
- render user-friendly error message

## Function: `clearInlineError`
```javascript
function clearInlineError() { ... }
```

## Function: `createPaymentIntent`
```javascript
async function createPaymentIntent(payload) { ... }
```
Responsibilities:
- POST to `/api/donations/create-payment-intent`
- parse JSON response
- throw friendly error on non-OK response

## Function: `confirmDonationPayment`
```javascript
async function confirmDonationPayment(clientSecret) { ... }
```
Responsibilities:
- call Stripe Elements confirm method
- return structured result

## Function: `handleDonationSubmit`
```javascript
async function handleDonationSubmit(event) { ... }
```
Responsibilities:
1. prevent default submit
2. clear errors
3. collect payload
4. set submitting state
5. create payment intent
6. confirm payment via Stripe
7. redirect to success or error page with Teelo ref
8. unset submitting state if staying on page

## Function: `initDonatePage`
```javascript
function initDonatePage() { ... }
```
Responsibilities:
- locate DOM nodes
- wire preset buttons
- wire custom amount input
- wire countdown
- initialise Stripe
- mount Elements
- bind submit handler

## Optional function: `pollDonationStatus`
```javascript
async function pollDonationStatus(publicId) { ... }
```
Responsibilities:
- for success page only if you want confirmation polish
- hit `/api/donations/{public_id}/status`
- stop when `paid` or terminal state is reached

---

## 9) Error / exception classes

If Teelo has a pattern for custom app exceptions, follow it. If not, a small local pattern inside `donation_service.py` is fine.

Suggested exceptions:

### `DonationValidationError`
For:
- bad amount
- bad field lengths
- invalid email

### `DonationNotFoundError`
For:
- missing `public_id`

### `StripeWebhookVerificationError`
For:
- bad webhook signature

### `DonationFeatureDisabledError`
For:
- feature flag off

These do not need a separate file unless you want one; they can live in `donation_service.py` if kept small.

---

## 10) Implementation sequence checklist

## Phase A — foundations
1. add config fields
2. add DB models
3. generate migration

## Phase B — backend core
4. add `schemas/donation.py`
5. add `donation_service.py` constants and exceptions
6. implement normalisation/validation helpers
7. implement `create_donation_and_payment_intent`
8. implement `get_donation_status`

## Phase C — routing
9. wire page routes
10. wire create-payment-intent route
11. wire status route

## Phase D — frontend
12. build templates
13. build `donate.js`
14. wire Stripe Elements and submit flow
15. add countdown behaviour

## Phase E — webhook + reconciliation
16. implement webhook verification helper
17. implement webhook event recording
18. implement event-specific handlers
19. wire webhook route

## Phase F — testing
20. unit tests
21. integration tests
22. Stripe test mode manual pass

---

## 11) Review checklist before implementation starts

Before code is written, verify the plan satisfies all product constraints:

- payment form stays on Teelo
- one-time only
- AUD only
- preset amounts + custom
- custom bounds A$5–A$1,000
- optional name/email/message
- anonymous option exists
- message max 240
- countdown near limit exists
- generic success page
- status endpoint exists
- Stripe receipt only
- funds framed as voluntary support for Cam’s work, usable at Cam’s discretion
- no charity/tax-deductible implication
- Stripe dashboard + DB enough for ops

---

## 12) My review of the planned structure

At this point, the structure is coherent and should work.

### What looks solid

- module boundaries fit Teelo’s existing architecture
- payment logic has a clear home
- public routes are the correct ownership layer
- DB model shape is sufficient without being overbuilt
- webhook idempotency is planned properly
- frontend responsibilities are separated from backend truth
- legal framing matches the actual product intent

### Main implementation risks to watch during coding

1. **Webhook vs UI race conditions**
   - already accounted for with status endpoint and generic success page

2. **Overcomplicating Stripe abstraction too early**
   - avoid unnecessary extra modules unless `donation_service.py` becomes unwieldy

3. **Client/server validation drift**
   - keep backend authoritative

4. **Accidental PII leakage on success/status endpoints**
   - status endpoint must stay minimal

5. **State transition bugs**
   - idempotent handlers and tests will matter here

### Conclusion

The plan is implementation-ready from a structural perspective. I do not currently see a design contradiction that would block the build, assuming the coding follows these module boundaries and webhook-first payment truth rules.
