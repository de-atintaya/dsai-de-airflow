# Databricks notebook source
my_catalog = 'g4_catalog'
my_prefix = 'MBRSA'

# COMMAND ----------

table_silver_prices = f'{my_catalog}.silver.{my_prefix}_market_prices_clean'
table_silver_holdings = f'{my_catalog}.silver.{my_prefix}_portfolio_positions'
table_gold_risk = f'{my_catalog}.gold.{my_prefix}_intraday_risk_monitor'

# COMMAND ----------

spark.sql(f"""DROP TABLE IF EXISTS {table_gold_risk}""")

spark.sql(f"""
    CREATE TABLE {table_gold_risk} AS
    WITH position_values AS (
        -- Calculamos el valor de cada asset en cada tick
        SELECT 
            p.market_time,
            h.account_id,
            p.asset_symbol,
            (p.price * h.quantity) as current_position_value
        FROM {table_silver_prices} p
        JOIN {table_silver_holdings} h 
          ON p.asset_symbol = h.asset_symbol
          -- Para la vida real se deberia calcular los holdings a traves del tiempo tambien, pero ahora estamos asumiendo que en cada
          -- tick se tiene el mismo holding 
    ),
    portfolio_total AS (
        -- Sumamos todo para conseguir el valor TOTAL del portafolio por timestamp
        SELECT 
            market_time,
            account_id,
            SUM(current_position_value) as total_portfolio_value
        FROM position_values
        GROUP BY market_time, account_id
    ),
    risk_calculations AS (
        -- Calculamos las metricas de riesgo (Drawdown y Volatility)
        SELECT
            market_time,
            account_id,
            total_portfolio_value,
            
            -- METRICA 1: Rolling Max Value (Ultimas 24 horas) - Simulado aca sobre toda la ventana
            MAX(total_portfolio_value) OVER (PARTITION BY account_id ORDER BY market_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as peak_value,
            
            -- METRICA 2: Rolling Volatility (Desviacion estandar sobre los ultimos ticks)
            STDDEV(total_portfolio_value) OVER (PARTITION BY account_id ORDER BY market_time ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) as volatility_stddev
            
        FROM portfolio_total
    )
    SELECT 
        market_time,
        account_id,
        round(total_portfolio_value, 2) as portfolio_value_usd,
        round(volatility_stddev, 2) as volatility,
        
        -- Drawdown %
        round(((total_portfolio_value - peak_value) / peak_value) * 100, 4) as drawdown_pct,
        
        -- LOGICA DE ALERTA:
        CASE 
            WHEN ((total_portfolio_value - peak_value) / peak_value) < -0.05 THEN 'HIGH RISK: STOP LOSS'
            WHEN volatility_stddev > 500 THEN 'HIGH VOLATILITY WARNING'
            ELSE 'NORMAL'
        END as risk_status
        
    FROM risk_calculations
    ORDER BY market_time DESC
""")

# COMMAND ----------

print("Gold Layer (Data Product) Created.")
display(spark.sql(f"SELECT * FROM {table_gold_risk}"))