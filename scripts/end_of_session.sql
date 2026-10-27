-- End of a trading session: orders still resting at the exchange expire there (DAY orders), so we cancel them here
-- and release the cash reserved for them. Also run automatically by the router when the (simulated) exchange
-- restarts, because a restarted exchange has forgotten every resting order.
--   docker exec -i hal-<leader> psql -U postgres -d trading < scripts/end_of_session.sql
BEGIN;
WITH o AS (
    SELECT id, account, reserve FROM orders
    WHERE status IN ('SUBMITTED', 'PARTIAL', 'CANCEL_REQUESTED')
    FOR UPDATE
), cancelled AS (
    UPDATE orders SET status = 'CANCELLED', reserve = 0, updated_at = now(),
                      note = 'session ended: order no longer at the exchange'
    FROM o WHERE orders.id = o.id
    RETURNING orders.id
), released AS (
    UPDATE accounts SET reserved = reserved - s.r
    FROM (SELECT account, sum(reserve) AS r FROM o GROUP BY account) s
    WHERE accounts.account = s.account
    RETURNING accounts.account
), events AS (
    INSERT INTO outbox (topic, key, msg_id, payload)
    SELECT 'orders.events', account, 'evt-cancelled-' || id,
           jsonb_build_object('kind', 'order.cancelled', 'order_id', id, 'account', account, 'why', 'session ended')
    FROM o
    ON CONFLICT (msg_id) DO NOTHING
    RETURNING 1
)
SELECT (SELECT count(*) FROM cancelled) AS orders_cancelled,
       (SELECT count(*) FROM released)  AS accounts_released,
       (SELECT count(*) FROM events)    AS events_written;
COMMIT;
