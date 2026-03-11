/*
============================================================================================================================
Stored Procedure: sales_clean_dataset()
============================================================================================================================
Purpose:
    Builds and refreshes clean-layer tables by integrating and standardizing data from fixed-layer sources.

Clean Layer Responsibilities:
    - Apply data quality rules and validation flags
    - Standardize formats, values, and reference data
    - Prepare canonical datasets for business-ready layers

Behavior:
    - Drops and fully rebuilds clean-layer tables
    - Logs execution timing for each table load

Execution Notes:
    - This procedure is non-incremental and idempotent
    - Intended to be run after fixed-layer refresh completes
============================================================================================================================
*/

CREATE OR REPLACE PROCEDURE sales_clean_dataset()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
BEGIN

    -- =====================================================
    -- Clean Customer Data
    -- =====================================================
	
    RAISE NOTICE '----------------------------------------------------------------------------------';
    RAISE NOTICE 'Loading Clean Customer Table';
    RAISE NOTICE '----------------------------------------------------------------------------------';

    start_time := clock_timestamp();

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
    
    end_time := clock_timestamp();
    RAISE NOTICE 'cl_cust loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    -- =====================================================
    -- Clean Sales Data
    -- =====================================================
	
    RAISE NOTICE '----------------------------------------------------------------------------------';
    RAISE NOTICE 'Loading Clean Sales Table';
    RAISE NOTICE '----------------------------------------------------------------------------------';

    start_time := clock_timestamp();

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

    end_time := clock_timestamp();
    RAISE NOTICE 'cl_sales loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    -- =====================================================
    -- Clean Marketing Data
    -- =====================================================
	
    RAISE NOTICE '----------------------------------------------------------------------------------';
    RAISE NOTICE 'Loading Clean Marketing Table';
    RAISE NOTICE '----------------------------------------------------------------------------------';

    start_time := clock_timestamp();

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
      
      CASE
          WHEN impressions IN ('many','', '-1') THEN 0
          ELSE impressions::INT
      END AS impressions,
        
      -- Flagging impressions
      CASE
          WHEN impressions IN ('many','') THEN 'invalid'
          WHEN impressions = '-1' THEN 'missing'
          ELSE 'valid'
      END AS impressions_flag
    	
    FROM raw_mark;

    end_time := clock_timestamp();
    RAISE NOTICE 'cl_mark loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    -- =====================================================
    -- DONE
    -- =====================================================
	
    RAISE NOTICE '----------------------------------------------------------------------------------';
    RAISE NOTICE 'ALL CLEAN TABLES LOADED SUCCESSFULLY';
    RAISE NOTICE '----------------------------------------------------------------------------------';

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'ERROR in sales_clean_dataset: %', SQLERRM;
END;
$$;
