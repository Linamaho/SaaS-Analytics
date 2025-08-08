-- Single staging model to clean all data needed for dashboard
-- Purpose: Prepare clean data for final dashboard metrics

{{ config(materialized='table') }}

WITH clean_monthly_revenue AS (
    SELECT 
        CAST(month AS DATE) AS month_date,
        STRFTIME(CAST(month AS DATE), '%Y-%m') AS month_str,
        CAST(COALESCE(new_mrr, 0) AS DECIMAL(12,2)) AS new_mrr,
        CAST(COALESCE(churned_mrr, 0) AS DECIMAL(12,2)) AS churned_mrr,
        CAST(COALESCE(expansion_mrr, 0) AS DECIMAL(12,2)) AS expansion_mrr,
        CAST(COALESCE(contraction_mrr, 0) AS DECIMAL(12,2)) AS contraction_mrr,
        CAST(COALESCE(net_mrr_growth, 0) AS DECIMAL(12,2)) AS net_mrr_growth
    FROM {{ ref('monthly_revenue') }}
    WHERE month IS NOT NULL
),

clean_customers AS (
    SELECT 
        customer_id,
        CAST(signup_date AS DATE) AS signup_date
    FROM {{ ref('customers') }}
    WHERE customer_id IS NOT NULL
),

clean_subscriptions AS (
    SELECT 
        customer_id,
        CAST(event_date AS DATE) AS event_date,
        TRIM(LOWER(event_type)) AS event_type
    FROM {{ ref('subscriptions') }}
    WHERE customer_id IS NOT NULL 
    AND event_date IS NOT NULL
    AND event_type = 'churn'
)

SELECT 
    -- Monthly revenue data
    mr.month_date,
    mr.month_str,
    mr.new_mrr,
    mr.churned_mrr,
    mr.expansion_mrr,
    mr.contraction_mrr,
    mr.net_mrr_growth,
    
    -- Customer counts for ARPU calculation
    COUNT(CASE 
        WHEN c.signup_date <= mr.month_date 
        AND NOT EXISTS (
            SELECT 1 FROM clean_subscriptions s 
            WHERE s.customer_id = c.customer_id 
            AND s.event_date <= mr.month_date
        ) 
        THEN 1 
    END) AS active_customers

FROM clean_monthly_revenue mr
CROSS JOIN clean_customers c
GROUP BY 
    mr.month_date, mr.month_str, mr.new_mrr, mr.churned_mrr, 
    mr.expansion_mrr, mr.contraction_mrr, mr.net_mrr_growth
ORDER BY mr.month_date