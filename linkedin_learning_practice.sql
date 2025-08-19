select * from red30techonlineretailsales;

select AVG(order_total) as avg_sales 
FROM red30techonlineretailsales

-- subquery
SELECT customertype, custname, custstate
FROM red30techonlineretailsales
WHERE order_total >= (select AVG(order_total) as avg_sales 
						FROM red30techonlineretailsales
);

-- CTE
-- learn, cte is temporary table. need to use "join" clause
-- why cross join? CTE creates only one row. Can't use left join cuz it won't match each row for comparsion.
WITH avg_total AS (
  		SELECT ROUND(AVG(order_total), 2) AS avg_sales 
  		FROM red30techonlineretailsales
)
SELECT r.customertype, r.custname, r.custstate, r.order_total, a.avg_sales
FROM red30techonlineretailsales r
CROSS JOIN avg_total a
WHERE r.order_total >= a.avg_sales
ORDER BY r.custstate, r.custname;

select * from red30techconferencesessioninfo

-- lead(fine next) lag(find previous) practice
SELECT start_date, end_date, session_name, room_name,
  LAG(start_date)  OVER (PARTITION BY room_name ORDER BY start_date) AS previous_session_time,
  LAG(session_name) OVER (PARTITION BY room_name ORDER BY start_date) AS previous_session,
  LEAD(start_date)  OVER (PARTITION BY room_name ORDER BY start_date) AS next_session_time,
  LEAD(session_name) OVER (PARTITION BY room_name ORDER BY start_date) AS next_session
FROM red30techconferencesessioninfo
-- WHERE room_name = 'Room 102'
ORDER BY start_date, room_name;

-- no need to use 'partition by if there is where clause filtered by room_name'
SELECT start_date, end_date, session_name, room_name,
  LAG(start_date)  OVER (ORDER BY start_date) AS previous_session_time,
  LAG(session_name) OVER (ORDER BY start_date) AS previous_session,
  LEAD(start_date)  OVER (ORDER BY start_date) AS next_session_time,
  LEAD(session_name) OVER (ORDER BY start_date) AS next_session
FROM red30techconferencesessioninfo
WHERE room_name = 'Room 102'
ORDER BY start_date, room_name;

-- optional, find gap minutes between sessions
SELECT
  start_date,
  end_date,
  session_name,
  room_name,
  ROUND(EXTRACT(EPOCH FROM (
    start_date - LAG(end_date) OVER (PARTITION BY room_name ORDER BY start_date)
  )) / 60 )AS gap_minutes
FROM red30techconferencesessioninfo
WHERE room_name = 'Room 102'
ORDER BY start_date;

-- linkedin Challenge
-- 
select * from red30techonlineretailsales
where prodcategory = 'Drones'

SELECT 
	orderdate, 
	quantity
	Lead(orderdate, 5) OVER (PARTITION BY procategory ORDER BY orderdate ASC) as next_order
FROM red30techonlineretailsales
WHERE procategory = 'Drones'
ORDER BY orderdate;

SELECT 
  orderdate, 
  quantity,
  LAG(orderdate, 5) OVER (PARTITION BY prodcategory ORDER BY orderdate ASC) AS previous_order
FROM red30techonlineretailsales
WHERE prodcategory = 'Drones'
ORDER BY orderdate;

--solution using cte and lag 
WITH order_by_days as (
				SELECT orderdate, SUM(quantity) as quantity_by_day
				FROM red30techonlineretailsales
				WHERE prodcategory = 'Drones'
				GROUP BY orderdate
)

SELECT orderdate, 
	   quantity_by_day,
	   LAG(quantity_by_day, 1) OVER (ORDER BY orderdate ASC) AS LastOrderQuantity_1,
	   LAG(quantity_by_day, 2) OVER (ORDER BY orderdate ASC) AS LastOrderQuantity_2,
	   LAG(quantity_by_day, 3) OVER (ORDER BY orderdate ASC) AS LastOrderQuantity_3,
	   LAG(quantity_by_day, 4) OVER (ORDER BY orderdate ASC) AS LastOrderQuantity_4,
	   LAG(quantity_by_day, 5) OVER (ORDER BY orderdate ASC) AS LastOrderQuantity_5
FROM order_by_days

-- --extensive learning
-- WITH order_by_days AS (
--   SELECT
--     orderdate::date AS order_date,
--     SUM(quantity)   AS quantity_by_day
--   FROM red30techonlineretailsales
--   WHERE prodcategory = 'Drones'
--   GROUP BY orderdate::date
-- )
-- SELECT
--   order_date,
--   quantity_by_day,
--   LAG(quantity_by_day, 1) OVER w AS last_qty_1,
--   LAG(quantity_by_day, 2) OVER w AS last_qty_2,
--   LAG(quantity_by_day, 3) OVER w AS last_qty_3,
--   LAG(quantity_by_day, 4) OVER w AS last_qty_4,
--   LAG(quantity_by_day, 5) OVER w AS last_qty_5,
--   quantity_by_day - LAG(quantity_by_day, 1) OVER w AS diff_from_prev,
--   ROUND(
--     (quantity_by_day::numeric / NULLIF(LAG(quantity_by_day, 1) OVER w, 0) - 1) * 100
--   , 2)                                                                AS pct_change
-- FROM order_by_days
-- WINDOW w AS (ORDER BY order_date)
-- ORDER BY order_date;


--- solution with cte and lead
WITH order_by_days as (
				SELECT orderdate, SUM(quantity) as quantity_by_day
				FROM red30techonlineretailsales
				WHERE prodcategory = 'Drones'
				GROUP BY orderdate
)

SELECT orderdate, 
	   quantity_by_day,
	   Lead(quantity_by_day, 1) OVER (ORDER BY orderdate ASC) AS LastOrderQuantity_1,
	   Lead(quantity_by_day, 2) OVER (ORDER BY orderdate ASC) AS LastOrderQuantity_2,
	   Lead(quantity_by_day, 3) OVER (ORDER BY orderdate ASC) AS LastOrderQuantity_3,
	   Lead(quantity_by_day, 4) OVER (ORDER BY orderdate ASC) AS LastOrderQuantity_4,
	   Lead(quantity_by_day, 5) OVER (ORDER BY orderdate ASC) AS LastOrderQuantity_5
FROM order_by_days

create table employeedirectory(
	fist_name text NOT NULL,
	last_name text NOT NULL,
	title text NOT NULL,
	department text NOT NULL,
	email text NOT NULL,
	DepartmentID SMALLINT NOT NULL,
	EmployeeID SMALLINT NOT NULL,
	Manager SMALLINT NOT NULL
);

-- rank and dense_rank
select *,
	rank() OVER (ORDER BY last_name) as rank_,
	Dense_rank() OVER (ORDER BY last_name) as dense_rank_
from employeedirectory


CREATE TABLE red30techconventionattendees (
    registration_date     TIMESTAMP NOT NULL,
    first_name            TEXT NOT NULL,
    last_name             TEXT NOT NULL,
    email                 TEXT NOT NULL,
    phone_number          TEXT NOT NULL,
    address               TEXT NOT NULL,
    city                  TEXT NOT NULL,
    state                 TEXT NOT NULL,
    zip                   INTEGER NOT NULL,
    package               TEXT NOT NULL,
    package_cost          SMALLINT NOT NULL,
    discount              TEXT,
    discount_amount       NUMERIC,
    amount_paid           NUMERIC NOT NULL,
    payment_type          TEXT NOT NULL,
    last_4_card_digits    SMALLINT NOT NULL,
    authorization_code    INTEGER NOT NULL
);

select * from red30techconventionattendees

ALTER TABLE red30techconventionattendees
ALTER COLUMN registration_date TYPE date
USING registration_date::date;

-- find first 3 registered attendees from each state
-- using rank() selected exactly first three registered attendees. 
WITH ranking AS (
  SELECT 
    first_name, 
    last_name, 
    state,
    registration_date,
    RANK() OVER (PARTITION BY state ORDER BY registration_date) AS rank_
  FROM red30techconventionattendees
)
SELECT first_name, last_name, state, registration_date, rank_
FROM ranking
WHERE rank_ <= 3
ORDER BY state, rank_;

-- using dense_rank(), there might be the fourth registration encounter in this calculation. 
-- some attendees might register on the same day in different time
WITH ranking AS (
  SELECT 
    first_name, 
    last_name, 
    state,
    registration_date,
    DENSE_RANK() OVER (PARTITION BY state ORDER BY registration_date) AS rank_
  FROM red30techconventionattendees
)
SELECT first_name, last_name, state, registration_date, rank_
FROM ranking
WHERE rank_ <= 3
ORDER BY state, rank_;


