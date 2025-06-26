from sodapy import Socrata
import os
import time
import logging

logging.basicConfig(level=logging.INFO)

DATASET_IDS = {
    "nyc_parks_events": "6v4b-5gp4",
    "nyc_permitted_events_historical": "bkfu-528j",
    "nyc_permitted_events_future": "tvpp-9vvx",
    "nyc_311_requests": "erm2-nwe9",
    "nyc_311_resolutions": "5ijn-vbdv",
    "linknyc_status": "n6c5-95xh",
    "nyc_sidewalk_status": "6kbp-uz6m",
    "nyc_tree_points": "hn5i-inap"
}

DOMAIN = "data.cityofnewyork.us"

# ==========================
# GENERAL NYC OPEN DATA UTILS
# ==========================
def get_client():
    """Get a Socrata client for NYC Open Data."""
    return Socrata(DOMAIN, None)

# ==========================
# FETCH DATA FUNCTION
# ==========================
def fetch_nyc_data(dataset_key, batch_size=1000, max_retries=3, sleep_sec=5):
    """fetch using pagination ($limit + $offset)."""
    client = get_client()
    all_events = []
    offset = 0
    dataset_id = DATASET_IDS[dataset_key]

    while True:
        attempt = 0
        while attempt < max_retries:
            try:
                results = client.get(dataset_id, limit=batch_size, offset=offset)
                if not results:
                    return all_events
                all_events.extend(results)
                
                logging.info(f"[{dataset_key}] fetched {len(results)} records. "
                             f"Offset now {offset + batch_size}.")
                
                offset += batch_size
                break
            except Exception as e:
                attempt += 1
                logging.warning(f"Error fetching batch (attempt {attempt}/{max_retries}): {e}")
                time.sleep(sleep_sec)

        if attempt == max_retries:
            logging.error("Max retries reached, stopping fetch.")
            return all_events
