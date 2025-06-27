import os
import logging
# import streamlit as st
# from line_jb.data_ingestion.search import fetch_posts_by_hashtag
from line_jb.data_ingestion.insert_manager import InsertManager
from line_jb.data_ingestion.fetch_nyc_open_data import fetch_nyc_data

REQUIRED_TABLES = [
    "linknyc_status",
    "nyc_parks_events", 
    "nyc_311_requests",
    "nyc_311_resolutions",
    "nyc_sidewalk_status",
    "nyc_tree_points",
    "nyc_permitted_events_future",
    "nyc_permitted_events_historical"
]

def get_insert_method_name(table_name: str) -> str:
    """
    Derives the insert method name from the table name.
    Strips 'nyc_' prefix if present, and prepends 'insert_'.
    """
    if table_name.startswith("nyc_"):
        base = table_name[len("nyc_"):]
    else:
        base = table_name
    return f"insert_{base}"

def main():
    max_batch = 1000
    db_path = "db/local.db"
    schema_path = "db/schema.sql"
    
    inserter = InsertManager(db_path)

    # Check if all tables exist
    missing_tables = [t for t in REQUIRED_TABLES if not inserter.table_exists(t)]
    if missing_tables:
        logging.info(f"Missing tables detected: {missing_tables}. Initializing schema...")
        InsertManager.initialize_database(db_path, schema_path)
    else:
        logging.info("All required tables found. Skipping schema initialization.")

    # Fetch and insert data for each dataset
    for table_name in REQUIRED_TABLES:
        dataset_key = table_name
        insert_method_name = get_insert_method_name(table_name)

        try:
            logging.info(f"Fetching dataset: {dataset_key}")
            data = fetch_nyc_data(dataset_key, batch_size=max_batch)
            insert_func = getattr(inserter, insert_method_name)
            insert_func(data)
        except Exception as e:
            logging.error(f"Failed to process {dataset_key}: {e}")

if __name__ == "__main__":
#    st.title("Instagram Hashtag Explorer 🔍")
#    hashtag = st.text_input("Enter a hashtag (without #):")
#
#    if st.button("Search"):
#        posts = fetch_posts_by_hashtag(hashtag)
#        for post in posts:
#            st.image(post.thumbnail_url)  # Show post images
#            st.write(post.caption or "")
    main()
