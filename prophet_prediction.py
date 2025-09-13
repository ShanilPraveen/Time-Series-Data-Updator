import pandas as pd
from sqlalchemy import create_engine, text
from prophet import Prophet
from dotenv import load_dotenv
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in environment variables. Please check your .env file.")

engine = create_engine(DATABASE_URL)

def get_predictions_for_channel(channel_id: str, periods_to_predict: int = 60, metric_to_predict: str = "subscriber_count"):
    logger.info(f"Starting prediction test for channel '{channel_id}'...")

    try:
        with engine.connect() as connection:
            query = text(f"""
                SELECT time, {metric_to_predict}
                FROM channel_metrics
                WHERE channel_id = :channel_id
                ORDER BY time ASC;
            """)
            
            df = pd.read_sql(query, connection, params={'channel_id': channel_id})

    except Exception as e:
        logger.error(f"Error retrieving data from database: {e}")
        return None

    if df.empty or len(df) < 2:
        logger.warning(f"Insufficient data points for channel '{channel_id}'. Found {len(df)} rows. Need at least 2.")
        return None

    logger.info(f"Retrieved {len(df)} data points for training the model.")

    df = df.rename(columns={'time': 'ds', metric_to_predict: 'y'})
    df['ds'] = pd.to_datetime(df['ds'])
    df['ds'] = df['ds'].dt.tz_localize(None)

    try:
        model = Prophet()
        model.fit(df)
        logger.info("Prophet model trained successfully.")
    except Exception as e:
        logger.error(f"Error training Prophet model: {e}")
        return None

    future = model.make_future_dataframe(periods=periods_to_predict)
    forecast = model.predict(future)

    predictions = forecast[forecast['ds'] > df['ds'].max()]
    
    logger.info(f"Generated {len(predictions)} predictions for the next {periods_to_predict} days.")

    print("\n--- Predictions for Upcoming Two Months ---")
    print(predictions[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_string())

    return predictions

if __name__ == "__main__":
    test_channel_id = "UCdJppX3uAMFMvCTuE9qCowg" 
    
    get_predictions_for_channel(
        channel_id=test_channel_id,
        periods_to_predict=60,
        metric_to_predict="subscriber_count"
    )