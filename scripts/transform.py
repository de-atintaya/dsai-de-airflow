import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
  df['transaction_date'] = pd.to_datetime(df['transaction_date'])

  df['year'] = df['transaction_date'].dt.year
  df['month'] = df['transaction_date'].dt.month
  df['quarter'] = df['transaction_date'].dt.quarter
  df['day_of_week'] = df['transaction_date'].dt.day_name()
  df['is_weekend'] = df['transaction_date'].dt.weekday >= 5

  df['transaction_category'] = df['transaction_type'].map({
      'DEPOSIT': 'Income',
      'WITHDRAWAL': 'Expense',
      'TRANSFER': 'Transfer',
      'PAYMENT': 'Payment'
  }).fillna('Other')

  df['amount_abs'] = df['amount'].abs()
  df['is_large_transaction'] = df['amount_abs'] > 1000
  df['weekend_transaction'] = df['is_weekend']
  df['high_value_flag'] = df['amount_abs'] > df['amount_abs'].quantile(0.95)

  return df
