\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '90s';
WITH cash_flow AS (
    SELECT o.account, sum(CASE WHEN o.side='BUY' THEN f.qty*f.price ELSE -f.qty*f.price END) AS spent
    FROM fills f JOIN orders o ON o.id=f.order_id GROUP BY o.account
), quantities AS (
    SELECT o.account, o.symbol, sum(CASE WHEN o.side='BUY' THEN f.qty ELSE -f.qty END) AS qty
    FROM fills f JOIN orders o ON o.id=f.order_id GROUP BY o.account, o.symbol
), reservations AS (
    SELECT account, sum(reserve) AS reserved FROM orders GROUP BY account
)
SELECT json_build_object(
    'checked_at', now(),
    'cash_wrong', (SELECT count(*) FROM accounts a LEFT JOIN cash_flow c USING(account)
                   WHERE abs(a.cash-(a.initial_cash-COALESCE(c.spent,0))) > 0.01),
    'positions_wrong', (SELECT count(*) FROM quantities q FULL JOIN positions p USING(account,symbol)
                        WHERE COALESCE(q.qty,0) <> COALESCE(p.qty,0)),
    'reserve_wrong', (SELECT count(*) FROM accounts a LEFT JOIN reservations r USING(account)
                      WHERE abs(a.reserved-COALESCE(r.reserved,0)) > 0.01),
    'overfilled', (SELECT count(*) FROM orders WHERE filled > qty),
    'total_orders', (SELECT count(*) FROM orders),
    'total_fills', (SELECT count(*) FROM fills)
);
COMMIT;
