/*
============================================================================================================================
DDL Script: Create Clean Layer Tables
============================================================================================================================
Purpose:
    Standardize and clean raw source data into structured clean-layer tables for downstream analytics and modeling.

Tables Created:
    cl_cust   - cleaned customer dataset
    cl_sales  - cleaned sales transactions
    cl_mark   - cleaned marketing campaign data

Source Tables:
    raw_cust
    raw_sales
    raw_mark

Execution:
    Run this script to re-define and align the structure of all clean-layer tables.
============================================================================================================================
*/

----------------------------------------------
		-->> Clean Customer Data <<--
----------------------------------------------
DROP TABLE IF EXISTS cl_cust CASCADE;

CREATE TABLE cl_cust AS
SELECT
    customer_id,
    COALESCE(INITCAP(TRIM(customer_name)), 'N/A') AS customer_name,
    COALESCE(email, 'N/A') AS email,
    COALESCE(phone, 'N/A') AS phone,
    COALESCE(city, 'N/A') AS city,
    COALESCE(state, 'N/A') AS state,
    COALESCE(country, 'N/A') AS country
FROM raw_cust;

----------------------------------------------
		-->> Clean Sales Data <<--
----------------------------------------------
DROP TABLE IF EXISTS cl_sales CASCADE;

CREATE TABLE cl_sales AS
SELECT
	sale_id,
	customer_id,
	product_cleaned AS product,
	quantity_cleaned AS quantity,
	price,
	price * COALESCE(quantity_cleaned,0) AS total_sales,
	CASE
		WHEN quantity_cleaned = 0 THEN 'no sale'
		WHEN quantity_cleaned < 0 THEN 'return'
		ELSE 'sale'
	END AS transaction_type,

	CASE 
		WHEN quantity_cleaned < 0 THEN ABS(quantity_cleaned)
		ELSE 0
	END AS returned_units,
	
	quantity_flag,
	sale_date
FROM(
	SELECT
		sale_id,
		customer_id,
		COALESCE(
			CASE
				WHEN INITCAP(TRIM(product)) ='NULL' THEN NULL
				WHEN product ~ '^[A-Za-z0-9 ]{3,}$'
				THEN TRIM(product)
				ELSE NULL
			END,
			'N/A'	
		) AS product_cleaned,

		CASE
			WHEN TRIM(quantity) ~ '^-?[0-9]+$'
			THEN TRIM(quantity)::INT
			ELSE 0
		END AS quantity_cleaned,

		-- Quantity Flag
		CASE
			WHEN quantity = 'unknown' OR TRIM(quantity) = '' THEN 'missing'
			ELSE 'valid'
		END AS quantity_flag,
		
		price,
		sale_date
	FROM raw_sales
) t;

----------------------------------------------
		-->> Clean Marketing Data <<--
----------------------------------------------
DROP TABLE IF EXISTS cl_mark CASCADE;

CREATE TABLE cl_mark AS
SELECT
    customer_id,
	
    COALESCE(
		INITCAP(
			TRIM(
				NULLIF(campaign_name, '')
			)
		),
	'N/A') AS campaign_name,
	
    COALESCE(
		INITCAP(
			TRIM(
				NULLIF(channel, '')
			)
		),
	'N/A') AS channel,
	
	COALESCE(
	    CASE
	        WHEN budget = 'unknown' OR budget = '' THEN NULL
	        ELSE budget::INT
	    END,
	0) AS budget,

	-- Flagging Budget
	CASE
		WHEN budget = 'unknown' OR budget = '' THEN 'missing'
		ELSE 'valid'
	END AS budget_flag,
	
    COALESCE(clicks, 0) AS clicks,
	
    COALESCE(
		NULLIF(
			NULLIF(
				NULLIF(impressions,'many'),
			'') ,
		'-1')::INT,
	0) AS impressions,
    
    -- Flagging impressions
    CASE
        WHEN impressions = 'many' OR impressions = '' THEN 'invalid'
        WHEN impressions = '-1' THEN 'missing'
        ELSE 'valid'
    END AS impressions_flag
	
FROM raw_mark;
