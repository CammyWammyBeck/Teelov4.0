# Stripe Donations Implementation Plan

Status: Planning only — no code changes approved yet.
Owner: Cam / Barry
Date: 2026-04-11

## Goal

Add a secure one-time support flow to Teelo on Teelo’s own website using Stripe, with the payment form staying fully on Teelo via Stripe Elements.

This is not a temporary stopgap plan. It is the intended implementation direction.

---

## Locked product decisions

- **Payment UX:** Stripe Elements + Payment Intents
- **Site/product wording:** `Support Teelo`
- **Payment type:** one-time only
- **Currency:** AUD only
- **Preset amounts:** `$5 / $10 / $25 / $50 + custom`
- **Donor fields:** name optional, email optional, message optional
- **Anonymous support:** supported as an option
- **Receipts:** Stripe receipt only
- **Thank-you flow:** handled in Teelo UI
- **Ops/admin:** Stripe dashboard + DB inspection; no dedicated admin page required initially
- **Refund posture:** case-by-case

---

## Legal / product framing

This section matters because the wording on the page should match what the payment actually is.

### What this support is

Users are providing **voluntary financial support** for Cam and Cam’s work on Teelo.

That means:

- support may help Cam continue building and maintaining Teelo
- funds are **paid to Cam**
- funds are **not ringfenced** to development, hosting, data, or operating costs
- funds may be used at Cam’s discretion, including development, business costs, groceries, savings, or anything else

### What this support is not

The page should not imply any of the following unless they are actually true:

- charitable donation status
- tax deductibility
- nonprofit / charity status
- that all funds go directly back into Teelo
- investor rights, ownership, equity, or governance rights
- a promise to deliver particular features, timelines, or service levels

### Recommended framing language

**Long form:**

> Support Teelo with a one-time contribution. This is voluntary financial support for Cam and Cam’s ongoing work on Teelo. Funds may be used for Teelo’s development and operating costs, or at Cam’s discretion for other purposes, including personal expenses or savings. Contributions are not charitable donations, are not tax deductible, and do not create ownership rights or an obligation to deliver specific features or timelines.

**Short form:**

> Support Teelo with a one-time contribution. Support is voluntary and goes directly to Cam and Cam’s work. Funds may be used at Cam’s discretion.

---

## Technical recommendation

Use **Stripe Elements + Payment Intents**.

This keeps the payment form fully on Teelo while still ensuring raw card details are handled by Stripe’s client-side payment components rather than Teelo’s backend.

### Why this is the correct approach

- keeps the payment experience native to Teelo
- avoids redirecting users to a hosted Stripe checkout page
- gives full control over form structure and page copy
- still preserves a sane security posture if implemented properly

### Trade-off accepted

Compared with Stripe Checkout, this introduces:

- more frontend work
- more payment states to handle
- more implementation detail around success/failure UX

That trade-off is acceptable because the product requirement is explicit: the payment form should live on Teelo.

---

## Non-negotiable security requirements

These are mandatory.

1. **HTTPS only**
   - Donation pages and API endpoints must run over HTTPS in production.

2. **No raw card data through Teelo servers**
   - Card details must only be collected via Stripe Elements.
   - Teelo backend must never accept or log raw PAN/CVC/expiry values.

3. **Webhook signature verification**
   - Verify all Stripe webhook payloads using the Stripe webhook secret.
   - Reject invalid signatures.

4. **Server-side amount validation**
   - Never trust client-submitted amount, currency, or mode.
   - Validate everything server-side before creating a Payment Intent.

5. **Idempotent webhook processing**
   - Stripe may retry webhooks.
   - Processing must be safe to repeat without duplicating side effects.

6. **Secret management**
   - Stripe secret keys and webhook secrets live in environment variables only.
   - Never commit secrets.

7. **Minimal data retention**
   - Collect and store only what is needed for donor UX and payment traceability.

8. **Rate limiting / abuse protection**
   - Rate-limit the create-payment-intent endpoint.
   - Consider bot protection later if abuse appears.

9. **Structured payment logs**
   - Log donation IDs, Stripe object IDs, state transitions, and webhook failures.
   - Do not log sensitive payment payloads unnecessarily.

10. **Webhook is source of truth**
   - UI success is not payment truth.
   - A donation is considered paid only after verified Stripe event processing.

---

## Proposed implementation architecture

Teelo already uses:

- FastAPI web app
- SQLAlchemy models
- Alembic migrations
- server-rendered web pages

This fits the donation flow well.

### High-level flow

1. User loads `/donate`
2. Teelo renders a support page and loads Stripe.js / Elements
3. User selects amount and fills optional fields
4. Frontend POSTs to Teelo to create a donation record + Stripe Payment Intent
5. Backend returns `client_secret` + Teelo donation reference
6. Frontend confirms payment using Stripe Elements
7. Frontend navigates to success/failure UI depending on client result
8. Stripe sends webhook to Teelo
9. Teelo verifies webhook and marks donation `paid` / `failed` / `refunded` as appropriate
10. Teelo UI reflects the verified outcome

### Important implementation principle

The frontend may show an optimistic success state after Stripe confirmation returns, but the database must still be finalised by webhook processing.

---

## Database design

## Table 1 — `donations`

This is the primary internal record of each support payment attempt.

### Recommended columns

- `id` — primary key
- `public_id` — short external-safe identifier (UUID or random token)
- `status` — enum/string
  - `created`
  - `pending`
  - `paid`
  - `failed`
  - `cancelled`
  - `refunded`
- `currency` — should be `aud`
- `amount_cents` — integer, server-validated
- `is_custom_amount` — boolean
- `donor_name` — nullable
- `donor_email` — nullable
- `donor_message` — nullable
- `is_anonymous` — boolean
- `stripe_payment_intent_id` — nullable until created
- `stripe_customer_id` — nullable
- `stripe_latest_charge_id` — nullable
- `stripe_last_event_id` — nullable
- `payment_method_type` — nullable, useful for analytics/ops
- `paid_at` — nullable datetime
- `failed_at` — nullable datetime
- `refunded_at` — nullable datetime
- `created_at`
- `updated_at`

### Recommended indexes / constraints

- unique index on `public_id`
- unique index on `stripe_payment_intent_id` where not null
- index on `status`
- index on `created_at`
- index on `paid_at`

### Notes

- `public_id` gives Teelo a safe reference usable in UI/query params without exposing internal IDs.
- `is_custom_amount` helps understand preset-vs-custom usage later without overcomplicating the model.

## Table 2 — `stripe_webhook_events`

Recommended for idempotency, debugging, and reconciliation.

### Recommended columns

- `id` — primary key
- `stripe_event_id` — unique
- `event_type`
- `object_id` — e.g. Payment Intent ID if useful
- `processing_status`
  - `received`
  - `processed`
  - `ignored`
  - `failed`
- `related_donation_id` — nullable FK to `donations.id`
- `error_text` — nullable
- `received_at`
- `processed_at` — nullable

### Why this table is worth keeping

Stripe retries. Humans misread logs. Bugs happen. This table makes payment-event handling inspectable and repeat-safe.

---

## Donation state model

Recommended lifecycle:

1. `created`
   - Teelo has accepted the request but Stripe object is not yet confirmed

2. `pending`
   - Payment Intent exists and payment confirmation is underway / awaiting outcome

3. `paid`
   - Verified webhook says payment succeeded

4. `failed`
   - Verified webhook indicates failure, or Teelo concludes the intent failed

5. `cancelled`
   - Optional internal state if the user abandons before completion and Teelo chooses to represent that

6. `refunded`
   - Payment later refunded

### Canonical rule

Only webhook-confirmed Stripe events should move a donation into `paid` or `refunded`.

---

## Validation rules

These should be explicit in the implementation.

## Amount rules

### Presets

- A$5
- A$10
- A$25
- A$50

### Custom amount

Confirmed bounds:

- minimum custom amount: **A$5**
- maximum custom amount: **A$1,000**

### Server-side validation rules

- currency must equal `aud`
- amount must be integer cents
- amount must be positive
- amount must fall within configured min/max bounds
- client cannot submit arbitrary currency or recurring mode

## Field rules

### `donor_name`
- optional
- trim whitespace
- store null if empty
- recommended max length: **80** characters

### `donor_email`
- optional
- trim + lowercase normalisation if appropriate
- validate format if present
- store null if empty
- recommended max length: **254** characters

### `donor_message`
- optional
- trim whitespace
- store null if empty
- max length: **240** characters
- frontend should show a character countdown when the user is getting near the limit
- must be safely escaped/rendered anywhere in UI

### `is_anonymous`
- required boolean
- defaults to `false`

### Anonymous behaviour recommendation

Because there is no public donor wall, anonymous is mostly a preference about how Teelo conceptually treats the support, not a public display concern.

Recommended behaviour:
- user can tick “Support anonymously”
- name/email/message fields remain available
- if `is_anonymous = true`, Teelo still stores submitted fields for payment/support context
- but any future public display must suppress personally identifying information unless the user explicitly opted in elsewhere

This gives flexibility without creating weird UI constraints now.

---

## API / route spec

Exact filenames and router placement can follow Teelo conventions.

## Public pages

### `GET /donate`
Render the Support Teelo page.

Responsibilities:
- render donation copy
- show amount selector
- show optional donor fields
- render anonymous toggle
- load Stripe publishable key / client config needed for Elements
- render placeholders for validation and payment status

### `GET /donate/success`
Render a thank-you page.

Recommended behaviour:
- accept Teelo `public_id` in query string if useful
- show **generic** success copy even if donor name was provided
- do not expose sensitive payment details
- if webhook finalisation is not yet visible, page can show “processing/confirming” language defensively

### `GET /donate/error`
Render a retry/failure page.

Recommended behaviour:
- explain the payment did not complete
- provide path back to `/donate`
- avoid claiming a charge definitely failed unless Teelo knows that from Stripe state

## API endpoints

### `POST /api/donations/create-payment-intent`
Create a Teelo donation row and a Stripe Payment Intent.

#### Expected request payload

```json
{
  "amount_cents": 1000,
  "currency": "aud",
  "donor_name": "Optional",
  "donor_email": "optional@example.com",
  "donor_message": "Optional",
  "is_anonymous": false
}
```

#### Backend responsibilities

- validate payload
- normalise optional fields
- create internal donation row in `created` state
- create Stripe Payment Intent with safe metadata
- update donation with Stripe identifiers and move to `pending`
- return response containing:
  - `public_id`
  - `client_secret`
  - safe display metadata if needed

#### Stripe metadata recommendation

Attach safe metadata such as:
- `teelo_donation_public_id`
- `teelo_mode=one_time`
- maybe `is_anonymous=true/false`

Do **not** put sensitive freeform content into Stripe metadata unless there is a good reason.

### Optional endpoint: `GET /api/donations/:public_id/status`

Chosen: **yes, include this endpoint**.

Purpose:
- lets the success page poll or refresh verified donation status from Teelo
- avoids relying solely on client-side confirmation results

Possible response:

```json
{
  "public_id": "abc123",
  "status": "paid"
}
```

This is useful for robust UX.

## Webhook endpoint

### `POST /api/stripe/webhook`

#### Responsibilities

- read raw request body
- verify Stripe signature
- record event in `stripe_webhook_events`
- ignore already-processed event IDs safely
- map Stripe object back to Teelo donation row
- update donation state idempotently
- record processing result

#### Relevant events

For this project, implementation should handle at least:

- `payment_intent.succeeded`
- `payment_intent.payment_failed`
- `charge.refunded`

### Canonical event mapping

- `payment_intent.succeeded` → donation `paid`
- `payment_intent.payment_failed` → donation `failed`
- `charge.refunded` → donation `refunded`

---

## Frontend flow spec

## Donation form structure

Recommended sections:

1. headline + explanation
2. preset amount selector
3. custom amount input
4. optional donor fields
5. anonymous checkbox
6. legal/support framing note
7. Stripe Elements payment element
8. submit button
9. inline error/status area

## Form behaviour

### Amount selection

- clicking a preset amount selects it and clears custom amount
- entering a custom amount deselects preset buttons visually
- only one effective amount source at a time

### Optional fields

- empty optional fields should submit as null/empty and be normalised server-side

### Anonymous toggle

Recommended UX:
- simple checkbox: `Support anonymously`
- leave optional fields visible
- optionally show a brief helper note like: `Your support can still include contact details for receipts or support context.`

### Submit behaviour

On submit:
- disable the button
- show processing state
- call `create-payment-intent`
- mount/use Stripe Elements confirmation flow
- handle success/error states cleanly
- do not allow duplicate rapid submissions

## Success UX

Recommended behaviour:
- redirect or navigate to `/donate/success?ref=<public_id>`
- success page thanks the supporter
- page may optionally check `/api/donations/:public_id/status` until it sees `paid`, then settle

## Failure UX

Recommended behaviour:
- show friendly inline error or route to `/donate/error`
- preserve amount and optional fields if feasible
- do not create duplicate intents unnecessarily on blind retries

---

## Stripe implementation notes

## Payment Intent creation

Recommended fields at creation time:

- `amount`
- `currency=aud`
- `automatic_payment_methods` enabled if appropriate for Stripe account setup
- metadata containing Teelo donation reference
- receipt email if donor email exists and you want Stripe to use it

### Receipt handling

Because the decision is **Stripe receipt only**, the implementation should pass donor email to Stripe when available and useful for receipt delivery.

### Payment method scope

If you want the cleanest initial implementation, keep the form focused on whatever Stripe Elements offers cleanly for your Stripe account/region.

---

## Settings / environment variables

Plan to add:

- `stripe_publishable_key`
- `stripe_secret_key`
- `stripe_webhook_secret`
- `stripe_currency_default` (default `aud`)
- `stripe_donations_enabled`
- `stripe_custom_amount_min_cents`
- `stripe_custom_amount_max_cents`
- `site_base_url`

Optional:
- preset amounts config if you want them configurable, though hardcoding them is also fine for now if that is Teelo’s product choice

---

## File-level implementation targets

These are planning targets only.

### Likely backend files

- `src/teelo/config.py`
  - Stripe settings
- `src/teelo/db/models.py`
  - `Donation`
  - `StripeWebhookEvent`
- `alembic/versions/...`
  - migration
- `src/teelo/web/routers/public.py` or similar
  - support page + API routes
- handler/service files under `src/teelo/web/` or `src/teelo/services/`
  - create-payment-intent logic
  - webhook logic
  - donation status lookup logic

### Likely frontend/template files

- donation page template
- success page template
- error page template
- JS module for amount selection + Stripe Elements flow

### Likely tests

- unit tests for validation and webhook processing
- integration tests for routes and donation state transitions

---

## Testing plan

## Unit tests

- amount preset and custom validation
- min/max amount enforcement
- email validation when present
- field normalisation rules
- anonymous flag behaviour
- webhook signature verification
- idempotent event processing
- donation state transitions

## Integration tests

- `/donate` renders correctly
- invalid create-payment-intent payload rejected
- valid create-payment-intent payload creates donation + Stripe object mock
- duplicate webhook event ignored safely
- `payment_intent.succeeded` marks donation paid
- `payment_intent.payment_failed` marks donation failed
- `charge.refunded` marks donation refunded
- status endpoint returns current verified state

## Manual test checklist

Using Stripe test mode:

- preset amount successful payment
- custom amount successful payment
- invalid custom amount blocked
- anonymous support flow
- optional fields empty flow
- optional fields populated flow
- Stripe receipt delivered when email provided
- no duplicate charges on rapid submit attempts
- success page handles slow webhook finalisation gracefully
- failure page returns user to retry cleanly
- mobile UI pass

---

## Rollout plan

1. Finalise remaining implementation-detail decisions
2. Implement behind feature flag using Stripe test mode
3. Verify end-to-end locally/staging
4. Configure production Stripe keys and webhook secret
5. Soft launch and monitor logs + Stripe dashboard
6. Public launch

---

## Remaining implementation-detail decisions for Cam

Most implementation details are now confirmed.

### Confirmed

1. **Custom amount minimum**
   - **A$5**

2. **Custom amount maximum**
   - **A$1,000**

3. **Name length limit**
   - Recommended working limit remains **80 chars** unless changed later

4. **Message length limit**
   - **240 chars**
   - frontend should show a countdown when the user is near the limit

5. **Success page style**
   - **Generic thank-you copy**

6. **Status endpoint**
   - **Yes**

### Still worth confirming during implementation

1. **Countdown behaviour threshold**
   - Recommendation: only show the visible countdown once the user is within the last **40–60 characters**

2. **Failure UX**
   - Recommendation: return user to a clean retry state with previously entered non-card values preserved if practical

---

## Barry’s concrete implementation recommendation

If you approve the remaining detail decisions above, the actual build should:

- use Stripe Elements with Payment Intents
- create a `donations` table and a `stripe_webhook_events` table
- treat webhooks as canonical payment truth
- provide a public support page, success page, and error page
- keep donor fields optional
- support anonymous contributions
- frame the payment as voluntary support for Cam’s work on Teelo, with funds usable at Cam’s discretion

That gives you the product you actually want, without pretending it’s a charity flow or a half-baked temporary compromise.
