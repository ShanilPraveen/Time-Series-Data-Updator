import os
from dotenv import load_dotenv
from datetime import datetime
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

TIMESCALEDB_HOST = os.getenv("TIMESCALEDB_HOST")
TIMESCALEDB_PORT = os.getenv("TIMESCALEDB_PORT")
TIMESCALEDB_DB = os.getenv("TIMESCALEDB_DB")
TIMESCALEDB_USER = os.getenv("TIMESCALEDB_USER")
TIMESCALEDB_PASSWORD = os.getenv("TIMESCALEDB_PASSWORD")

if not all([MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION_NAME,
            TIMESCALEDB_HOST, TIMESCALEDB_PORT, TIMESCALEDB_DB,
            TIMESCALEDB_USER, TIMESCALEDB_PASSWORD]):
    raise ValueError("One or more environment variables are not set.")

def get_mongo_client():
    try:
        client = MongoClient(MONGO_URI)
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None
    
def get_timescaledb_connection():
    try:
        conn = psycopg2.connect(
            host=TIMESCALEDB_HOST,
            port=TIMESCALEDB_PORT,
            dbname=TIMESCALEDB_DB,
            user=TIMESCALEDB_USER,
            password=TIMESCALEDB_PASSWORD,
            sslmode='require',
        )
        return conn
    except Exception as e:
        print(f"Error connecting to TimescaleDB: {e}")
        return None
    
def ingest_channel_metrics():
    print(f"Starting channel metrics ingestion at {datetime.now()}")

    mongo_client = None
    tsdb_conn = None
    try:
        mongo_client = get_mongo_client()
        if not mongo_client:
            return

        mongo_db = mongo_client[MONGO_DB_NAME]
        channels_collection = mongo_db[MONGO_COLLECTION_NAME]

        channels_data = channels_collection.find({}, {
            "data.channel_info.id": 1,
            "data.channel_info.statistics.viewCount": 1,
            "data.channel_info.statistics.subscriberCount": 1,
            "data.channel_info.statistics.videoCount": 1
        })

        data_to_insert = []
        current_timestamp = datetime.now() 

        for channel_doc in channels_data:
            try:
                channel_info = channel_doc.get("data", {}).get("channel_info", {})
                
                channel_id = channel_info.get("id")
                statistics = channel_info.get("statistics", {})

                view_count = int(statistics.get("viewCount", 0))
                subscriber_count = int(statistics.get("subscriberCount", 0))
                video_count = int(statistics.get("videoCount", 0))

                if channel_id: 
                    data_to_insert.append((
                        current_timestamp,
                        channel_id,
                        view_count,
                        subscriber_count,
                        video_count
                    ))
            except ValueError as ve:
                print(f"Warning: Could not convert statistic to int for channel {channel_doc.get('_id')}: {ve}")
            except Exception as e:
                print(f"Error processing channel document {channel_doc.get('_id')}: {e}")

        if not data_to_insert:
            print("No data collected from MongoDB for insertion. Exiting.")
            return

        tsdb_conn = get_timescaledb_connection()
        if not tsdb_conn:
            return

        tsdb_cursor = tsdb_conn.cursor()

        insert_query = """
            INSERT INTO channel_metrics (time, channel_id, view_count, subscriber_count, video_count)
            VALUES %s
            ON CONFLICT (time, channel_id) DO NOTHING;
        """

        execute_values(tsdb_cursor, insert_query, data_to_insert, page_size=1000)

        tsdb_conn.commit() 

        print(f"Successfully inserted {len(data_to_insert)} records into TimeScaleDB.")

    except Exception as e:
        print(f"An unexpected error occurred during ingestion: {e}")
        if tsdb_conn:
            tsdb_conn.rollback() 
    finally:
        if mongo_client:
            mongo_client.close()
            print("MongoDB connection closed.")
        if tsdb_conn:
            tsdb_cursor.close()
            tsdb_conn.close()
            print("TimeScaleDB connection closed.")


if __name__ == "__main__":
    ingest_channel_metrics()
    print(f"Ingestion completed at {datetime.now()}")