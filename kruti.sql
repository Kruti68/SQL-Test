1) Count of purchases per month (excluding refunded purchases)
 Only include rows with refund_time IS NULL

Solution:
SELECT
  date_trunc('month', purchase_time) AS month,
  COUNT(*) AS purchases_count
FROM transactions
WHERE refund_time IS NULL
GROUP BY 1
ORDER BY 1;


2) Number of stores that received at least 5 orders in Oct 2020
  // Use purchase_time date range for October 2020
  // If requirement wants to exclude refunds, add "AND refund_time IS NULL"

Solution:

SELECT COUNT(*) AS stores_with_5plus
FROM (
  SELECT store_id, COUNT(*) AS tx_count
  FROM transactions
  WHERE purchase_time >= '2020-10-01'::timestamp
    AND purchase_time <  '2020-11-01'::timestamp
  GROUP BY store_id
  HAVING COUNT(*) >= 5
) AS sub;


 3) For each store — shortest interval (in minutes) from purchase to refund time
  //we will Consider only refunded transactions (refund_time IS NOT NULL)
    // we will Use EXTRACT(EPOCH FROM ...) / 60 for minutes

Solution:

SELECT
  store_id,
  MIN(EXTRACT(EPOCH FROM (refund_time - purchase_time)) / 60.0) AS min_refund_minutes
FROM transactions
WHERE refund_time IS NOT NULL
  AND refund_time > purchase_time  -- sanity: only positive durations
GROUP BY store_id
ORDER BY store_id;


4) gross_transaction_value of every store's first order
   // For each store, find the earliest purchase_time and return its gross_


Solution: 
WITH first_per_store AS (
  SELECT
    store_id,
    transaction_id,            -- if present; else omit
    gross_transaction_value,
    purchase_time,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY purchase_time) AS rn
  FROM transactions
)
SELECT store_id, gross_transaction_value, purchase_time
FROM first_per_store
WHERE rn = 1
ORDER BY store_id;



5) Most popular item_name that buyers order on their first purchase
    // Steps:
//     1) determine each buyer's first purchase (by purchase_time)
//     2) join those item_ids to items.item_name (LEFT JOIN to keep unmatched)
//     3) count frequencies and pick the top item_name
//   Note: Many item_ids in transactions do not exist in items table => they will be NULL in item_name

Solution:

WITH first_purchase AS (
  SELECT buyer_id, item_id, purchase_time
  FROM (
    SELECT buyer_id, item_id, purchase_time,
           ROW_NUMBER() OVER (PARTITION BY buyer_id ORDER BY purchase_time) AS rn
    FROM transactions
  ) t
  WHERE rn = 1
)
SELECT
  i.item_name,
  COUNT(*) AS times_ordered
FROM first_purchase fp
LEFT JOIN items i
  ON i.item_id = fp.item_id
GROUP BY i.item_name
ORDER BY times_ordered DESC NULLS LAST
LIMIT 1;


 6) Create a flag indicating whether the refund can be processed
   // Business rule used: refund is processable if refund_time exists AND refund_time <= purchase_time + 72 hours
 // Show SELECT with computed flag (you can also ALTER TABLE ADD COLUMN + UPDATE to persist)


Solution:

SELECT
  transaction_id,        -- if available, else include buyer_id + purchase_time
  buyer_id,
  purchase_time,
  refund_time,
  gross_transaction_value,
  CASE
    WHEN refund_time IS NOT NULL
     AND refund_time <= purchase_time + INTERVAL '72 hours'
    THEN TRUE
    ELSE FALSE
  END AS refund_processable
FROM transactions;



 7) Create a rank by buyer_id and filter for only the second purchase per buyer
   // The instruction earlier said "ignore refunds" — we'll show both variants:
    //   A) ignoring refunded transactions (only count non-refunded purchases)
   //   B) counting all transactions (refunded or not)


    Solution: 

A) 
WITH ranked_non_refunded AS (
  SELECT t.*,
         ROW_NUMBER() OVER (PARTITION BY buyer_id ORDER BY purchase_time) AS rn
  FROM transactions t
  WHERE refund_time IS NULL
)
SELECT *
FROM ranked_non_refunded
WHERE rn = 2
ORDER BY buyer_id;

B) 
WITH ranked_all AS (
  SELECT t.*,
         ROW_NUMBER() OVER (PARTITION BY buyer_id ORDER BY purchase_time) AS rn
  FROM transactions t
)
SELECT *
FROM ranked_all
WHERE rn = 2
ORDER BY buyer_id;


8) Find the second transaction time per buyer
 //Use ROW_NUMBER over partition by buyer and pick rn = 2 (returns buyer + timestamp)


Solution: 


SELECT buyer_id, purchase_time AS second_purchase_time
FROM (
  SELECT buyer_id, purchase_time,
         ROW_NUMBER() OVER (PARTITION BY buyer_id ORDER BY purchase_time) AS rn
  FROM transactions
) t
WHERE rn = 2
ORDER BY buyer_id;
