-- The source of truth. Applied by control.py at start-up (idempotent).
CREATE TABLE IF NOT EXISTS accounts (
    account      text PRIMARY KEY,
    initial_cash numeric(18,2) NOT NULL,
    cash         numeric(18,2) NOT NULL,
    reserved     numeric(18,2) NOT NULL DEFAULT 0      -- money held for open BUY orders
);
CREATE TABLE IF NOT EXISTS positions (
    account text NOT NULL,
    symbol  text NOT NULL,
    qty     integer NOT NULL DEFAULT 0,
    PRIMARY KEY (account, symbol)
);
CREATE TABLE IF NOT EXISTS orders (
    id           bigserial PRIMARY KEY,
    client_ref   text NOT NULL UNIQUE,                  -- the trader's idempotency key: a resend can't make a 2nd order
    account      text NOT NULL,
    symbol       text NOT NULL,
    side         text NOT NULL,
    qty          integer NOT NULL,
    type         text NOT NULL,
    limit_price  numeric(18,2),
    status       text NOT NULL,                         -- PENDING SUBMITTING SUBMITTED PARTIAL FILLED CANCEL_REQUESTED CANCELLED EXPIRED REJECTED
    filled       integer NOT NULL DEFAULT 0,
    reserve      numeric(18,2) NOT NULL DEFAULT 0,      -- what is still reserved for this order
    exch_id      text,
    router_epoch integer,
    api_server   text,
    note         text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS orders_open ON orders (status) WHERE status IN ('PENDING','SUBMITTING','SUBMITTED','PARTIAL','CANCEL_REQUESTED');
CREATE INDEX IF NOT EXISTS orders_account ON orders (account, id DESC);
CREATE TABLE IF NOT EXISTS fills (
    exec_id  text PRIMARY KEY,                          -- the exchange's execution id: a fill is recorded once
    order_id bigint NOT NULL REFERENCES orders(id),
    qty      integer NOT NULL,
    price    numeric(18,2) NOT NULL,
    t        timestamptz NOT NULL DEFAULT now()
);
-- Transactional outbox: written in the SAME transaction as the business change, published to Kafka afterwards.
CREATE TABLE IF NOT EXISTS outbox (
    id           bigserial PRIMARY KEY,
    topic        text NOT NULL,
    key          text NOT NULL,
    msg_id       text NOT NULL UNIQUE,
    payload      jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    published_by text
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
-- One active order router: a lease with an epoch (fencing token).
CREATE TABLE IF NOT EXISTS router_lease (
    name       text PRIMARY KEY,
    holder     text,
    epoch      integer NOT NULL DEFAULT 0,
    expires_at timestamptz NOT NULL DEFAULT 'epoch'
);
INSERT INTO router_lease (name) VALUES ('router') ON CONFLICT DO NOTHING;
-- The router's positions in its input streams, saved in the same transaction as the work they cover.
CREATE TABLE IF NOT EXISTS router_state (
    name  text PRIMARY KEY,
    value bigint NOT NULL
);
-- Notifications delivered (deduplicated by event id).
CREATE TABLE IF NOT EXISTS notifications (
    event_id text PRIMARY KEY,
    kind     text,
    account  text,
    order_id bigint,
    worker   text,
    t        timestamptz NOT NULL DEFAULT now()
);

-- for the audit: recent-window queries stay fast as the tables grow
CREATE INDEX IF NOT EXISTS outbox_created ON outbox (created_at);
CREATE INDEX IF NOT EXISTS fills_order ON fills (order_id);
-- for the worker's retention job (deletes notifications older than 2 hours)
CREATE INDEX IF NOT EXISTS notifications_t ON notifications (t);
