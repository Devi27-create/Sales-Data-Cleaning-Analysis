/*
=========================================================================================
DDL Script: Create Raw Tables
=========================================================================================
Script Purpose:
    This script creates raw tables in the 'sales_data_clean' database.
    Existing tables are dropped and recreated.
=========================================================================================
*/

-- =========================
-- Raw Customers
-- =========================

DROP TABLE IF EXISTS raw_cust CASCADE;
CREATE TABLE raw_cust(
	customer_id INT PRIMARY KEY,
	customer_name VARCHAR(100),
	email VARCHAR(255),
	phone CHAR(12),
	city VARCHAR(50),
	state VARCHAR(100),
	country VARCHAR(50)
)

-- =========================
-- Raw Sales
-- =========================
  
DROP TABLE IF EXISTS raw_sales CASCADE;
CREATE TABLE raw_sales (
  sale_id INT PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES raw_cust(customer_id),
  product VARCHAR(100),
  quantity INT,
  price NUMERIC(8,2),
  sale_date DATE
);

-- =========================
-- Raw Marketing
-- =========================

DROP TABLE IF EXISTS raw_mark CASCADE;
CREATE TABLE raw_mark(
	customer_id INT NOT NULL REFERENCES raw_cust(customer_id),
	campaign_name VARCHAR(100),
	channel	VARCHAR(50),
	budget VARCHAR(50),
	clicks	INT,
	impressions VARCHAR(50)
);
