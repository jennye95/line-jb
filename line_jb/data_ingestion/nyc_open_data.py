from sodapy import Socrata
import os
import sqlite3
import time
import logging
from dateutil import parser as date_parser

logging.basicConfig(level=logging.INFO)

DATASET_ID = "6v4b-5gp4"
DOMAIN = "data.cityofnewyork.us"

# ==========================
# GENERAL NYC OPEN DATA UTILS
# ==========================
def get_client():
    """Get a Socrata client for NYC Open Data."""
    return Socrata(DOMAIN, None)

# ==========================
# SPECIFIC FUNCTION: PARKS EVENTS
# ==========================
def fetch_parks_events(batch_size=1000, max_retries=3, sleep_sec=5):
    """Fetch all NYC Parks events using pagination ($limit + $offset)."""
    client = get_client()
    all_events = []
    offset = 0

    while True:
        attempt = 0
        while attempt < max_retries:
            try:
                results = client.get(DATASET_ID, limit=batch_size, offset=offset)
                if not results:
                    return all_events
                all_events.extend(results)
                logging.info(f"Fetched {len(results)} records. Offset now {offset + batch_size}.")
                offset += batch_size
                break
            except Exception as e:
                attempt += 1
                logging.warning(f"Error fetching batch (attempt {attempt}/{max_retries}): {e}")
                time.sleep(sleep_sec)

        if attempt == max_retries:
            logging.error("Max retries reached, stopping fetch.")
            return all_events


def insert_parks_events(events, db_file=None):
	"""Insert park events into an SQLite database."""
	if db_file is None:
		db_file = os.path.join(os.path.dirname(__file__), '../../db/local.db')
		db_file = os.path.abspath(db_file)

	conn = sqlite3.connect(db_file)
	cur = conn.cursor()

	cur.execute("""
		CREATE TABLE IF NOT EXISTS nyc_parks_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_name TEXT,
			location TEXT,
			date_and_time REAL,
			borough TEXT,
			location_type TEXT,
			group_name_partner TEXT,
			event_type TEXT,
			category TEXT,
			attendance INTEGER,
			audience TEXT,
			source TEXT,
			UNIQUE(event_name, date_and_time, location)
		);
	""")

	for event in events:
		raw_date = event.get("date_and_time", "")
		try:
			parsed_date = date_parser.parse(raw_date).isoformat(sep=' ')
		except (ValueError, TypeError):
			parsed_date = None  # Or "1970-01-01 00:00:00" as a fallback

		cur.execute("""
			INSERT OR IGNORE INTO nyc_parks_events (
				event_name, location, date_and_time, borough,
				location_type, group_name_partner, event_type,
				category, attendance, audience, source
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
		""", (
			event.get("event_name", "N/A"),
			event.get("location", "N/A"),
			parsed_date,
			event.get("borough", "N/A"),
			event.get("locationtype", "N/A"),
			event.get("group_name_partner", "N/A"),
			event.get("event_type", "N/A"),
			event.get("category", "N/A"),
			int(float(event.get("attendance", 0))),
			event.get("audience", "N/A"),
			event.get("source", "N/A")
		))

	conn.commit()
	cur.close()
	conn.close()
