/*
============================================================================================================================
DDL Script: Create Business Layer 
============================================================================================================================
Purpose:
    The business layer delivers analytics-ready datasets built from validated
    clean tables. It applies business rules, aggregations, and derived
    calculations to produce meaningful metrics for reporting and analysis.

Data Pipeline Flow:
    Raw Layer  →  Clean Layer  →  Business Layer

Typical Outputs:
    - Aggregated sales metrics
    - Customer performance summaries
    - Marketing campaign performance datasets
==============================================================================================
*/

-- =========================
-- Sales Analytics View
-- =========================

DROP VIEW IF EXISTS sales_analytics_view CASCADE;

CREATE VIEW sales_analytics_view AS
-- Select the highest budget campaign per customer
WITH primary_campaign AS (
    SELECT DISTINCT ON (customer_id)
        customer_id,
        campaign_name,
        channel,
        budget,
		budget_flag,
        clicks,
        impressions,
        impressions_flag
    FROM cl_mark
    ORDER BY customer_id, budget DESC
)
SELECT
    ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.sale_date) AS purchase_sequence,
    c.customer_id,
    c.customer_name,
    c.email,
    c.phone,
    c.city,
    c.state,
    c.country,
    s.sale_id,
    s.product,
    s.quantity,
    s.price,
    s.total_sales,
	s.transaction_type,
	s.returned_units,
	s.quantity_flag,
    s.sale_date,
    pc.campaign_name,
    pc.channel,
    pc.budget,
	pc.budget_flag,
    pc.clicks,
    pc.impressions,
    pc.impressions_flag
FROM cl_sales AS s
LEFT JOIN cl_cust AS c ON s.customer_id = c.customer_id
LEFT JOIN primary_campaign AS pc ON s.customer_id = pc.customer_id;
