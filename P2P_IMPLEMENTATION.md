# P2P implementation

## Current behavior

- P2P orders support partial fills. Creating an order reserves only the requested quantity from the listing.
- A seller's USDC is locked when the ad is posted.
- Creating an order does not transfer USDC. The order starts as `pending_payment` with a 15-minute server-side deadline.
- The buyer can mark an order as `payment_made` or cancel it while payment is pending.
- The seller can release USDC only after the buyer marks the order paid.
- Release atomically debits/unlocks the seller, credits the buyer, completes the order, and writes ordered matching-engine outbox events.
- Either party can open an appeal after payment is marked. An authenticated administrator must release or cancel an appealed order.
- Buy creation is idempotent per buyer and idempotency key, including concurrent retries.
- USDC precision comes from shared token metadata (6 decimals); monetary storage remains raw integer `NUMERIC(38,0)`.
- An authenticated administrator sets the USDC/INR price for each India calendar day. Trading fails closed when today's price is absent.

## External limitations

- DEX.ai does not verify UPI, bank transfer, NEFT, or IMPS payments. “I have paid” and seller release are manual attestations.
- Payment account details and real-time P2P chat are not implemented.
- Appeals require manual evidence review outside this application; there is no automatic dispute decision.
- The backend sends a durable event ID and `Idempotency-Key` to the matching engine. Exact-once retry safety is only complete when the matching engine persists and deduplicates that key.
- Outbox delivery requires `MATCHING_ENGINE_URL` and `ENGINE_SHARED_SECRET`. When disabled, events remain in Postgres for later delivery or backfill.
