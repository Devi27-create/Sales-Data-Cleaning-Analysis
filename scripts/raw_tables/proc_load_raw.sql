/*
==============================================================================================
Stored Procedure: Load Raw Layer 
==============================================================================================
Script Purpose:
  This stored procedure loads data into the sales_data_cleaning database from external CSV files.
  It performs the following actions:
    - Drops tables before creating them.
    - Loads data from CSV files into the database using the COPY command.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage Example:
  CALL sales_raw_dataset;
==============================================================================================
*/

CREATE OR REPLACE PROCEDURE sales_raw_dataset()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN

    -- =========================
    -- Customer Data Raw
    -- =========================
	
    RAISE NOTICE '----------------------------------------------------------------------------------';
    RAISE NOTICE 'Loading Customer Raw Table';
    RAISE NOTICE '----------------------------------------------------------------------------------';

    start_time := clock_timestamp();

    -------------------------------
    -- Customer Raw Table Create
    -------------------------------

    DROP TABLE IF EXISTS raw_cust CASCADE;

    CREATE TABLE raw_cust(
    	customer_id INT PRIMARY KEY,
    	customer_name VARCHAR(100),
    	email VARCHAR(255),
    	phone CHAR(12),
    	city VARCHAR(50),
    	state VARCHAR(100),
    	country VARCHAR(50)
    );

    -----------------------------
    -- Customer Raw Table Load
    -----------------------------

    COPY raw_cust
	  FROM '/data/customers.csv'
	  DELIMITER ','
	  CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'raw_cust table loaded. Time taken: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- =========================
    -- Sales Data Raw
    -- =========================
	
    RAISE NOTICE '----------------------------------------------------------------------------------';
    RAISE NOTICE 'Loading Sales Raw Table';
    RAISE NOTICE '----------------------------------------------------------------------------------';

    start_time := clock_timestamp();

    -------------------------------
    -- Sales Raw Table Create
    -------------------------------

    DROP TABLE IF EXISTS raw_sales CASCADE;

    CREATE TABLE raw_sales (
      sale_id INT PRIMARY KEY,
      customer_id INT NOT NULL REFERENCES raw_cust(customer_id),
      product VARCHAR(100),
      quantity INT,
      price NUMERIC(8,2),
      sale_date DATE
    );

    -----------------------------
    -- Sales Raw Table Load
    -----------------------------

    COPY raw_sales
	  FROM '/data/sales.csv'
	  DELIMITER ','
	  CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'raw_sales table loaded. Time taken: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- =========================
    -- Marketing Data Raw
    -- =========================
	
    RAISE NOTICE '----------------------------------------------------------------------------------';
    RAISE NOTICE 'Loading Marketing Raw Table';
    RAISE NOTICE '----------------------------------------------------------------------------------';

    start_time := clock_timestamp();

    -------------------------------
    -- Marketing Raw Table Create
    -------------------------------

    DROP TABLE IF EXISTS raw_mark CASCADE;

    CREATE TABLE raw_mark(
    	customer_id INT NOT NULL REFERENCES raw_cust(customer_id),
    	campaign_name VARCHAR(100),
    	channel	VARCHAR(50),
    	budget VARCHAR(50),
    	clicks	INT,
    	impressions VARCHAR(50)
    );

    -----------------------------
    -- Marketing Raw Table Load
    -----------------------------

    COPY raw_mark
	  FROM '/data/marketing.csv'
	  DELIMITER ','
	  CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'raw_mark table loaded. Time taken: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    RAISE NOTICE '----------------------------------------------------------------------------------';
    RAISE NOTICE 'All Raw Tables Loaded Successfully';
    RAISE NOTICE '----------------------------------------------------------------------------------';

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error occurred in sales_raw_dataset: %', SQLERRM;
END;
$$;
