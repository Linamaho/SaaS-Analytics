-- Single dashboard table with all metrics needed
-- Purpose: MRR, Churn Rate and ARPU

{{ config(materialized='table') }}

WITH base_data AS (
    SELECT 
        month_date,
        month_str,
        new_mrr,
        churned_mrr,
        expansion_mrr,
        contraction_mrr,
        net_mrr_growth,
        active_customers,
        
        -- Calculate running total MRR
        SUM(net_mrr_growth) OVER (
            ORDER BY month_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS total_mrr
        
    FROM {{ ref('stg_all_data') }}
),

-- Calculate customer counts for churn rate
customer_churn_data AS (
    SELECT 
        month_date,
        month_str,
        
        -- Count customers who churned this month
        COUNT(CASE WHEN CAST(s.event_date AS DATE) >= month_date 
                   AND CAST(s.event_date AS DATE) < (month_date + INTERVAL 1 MONTH) 
                   THEN 1 END) AS churned_customers,
        
        -- Count customers active at beginning of month
        COUNT(CASE 
            WHEN CAST(c.signup_date AS DATE) < month_date 
            AND (s.event_date IS NULL OR CAST(s.event_date AS DATE) >= month_date)
            THEN 1 
        END) AS beginning_month_customers
        
    FROM {{ ref('stg_all_data') }} base
    CROSS JOIN {{ ref('customers') }} c
    LEFT JOIN {{ ref('subscriptions') }} s 
        ON c.customer_id = s.customer_id 
        AND TRIM(LOWER(s.event_type)) = 'churn'
    GROUP BY month_date, month_str
),

final_metrics AS (
    SELECT 
        bd.month_date,
        bd.month_str,
        bd.active_customers,
        bd.total_mrr,
        ccd.churned_customers,
        ccd.beginning_month_customers,
        
        -- MRR Churn Rate: churned MRR as % of previous month's total MRR
        CASE 
            WHEN LAG(bd.total_mrr, 1) OVER (ORDER BY bd.month_date) > 0 
            THEN ROUND(
                (bd.churned_mrr * 100.0) / LAG(bd.total_mrr, 1) OVER (ORDER BY bd.month_date), 
                2
            )
            ELSE 0 
        END AS mrr_churn_rate_pct,
        
        -- Customer Churn Rate: churned customers as % of beginning month customers  
        CASE 
            WHEN ccd.beginning_month_customers > 0 
            THEN ROUND(
                (ccd.churned_customers * 100.0) / ccd.beginning_month_customers, 
                2
            )
            ELSE 0 
        END AS customer_churn_rate_pct,
        
        -- ARPU: total MRR divided by active customers
        CASE 
            WHEN bd.active_customers > 0 
            THEN ROUND(bd.total_mrr / bd.active_customers, 2)
            ELSE 0 
        END AS overall_arpu
        
    FROM base_data bd
    LEFT JOIN customer_churn_data ccd ON bd.month_date = ccd.month_date
)

SELECT 
    month_date,
    month_str,
    
    -- For MRR Card & MRR Trend
    total_mrr,
    
    -- For MRR Churn Card & Trend (Revenue Impact)
    mrr_churn_rate_pct,
    
    -- For Customer Churn Card & Trend (Customer Count Impact)  
    customer_churn_rate_pct,
    
    -- For ARPU Card & ARPU Trend
    overall_arpu,
    
    -- Supporting data
    active_customers,
    churned_customers,
    beginning_month_customers

FROM final_metrics
WHERE active_customers > 0  -- Only months with customers

ORDER BY month_date