import os
import sqlite3
import time
import logging
from dateutil import parser as date_parser

__all__ = ["InsertManager"]

class InsertManager:
    def __init__(self, db_file):
        self.db_file = db_file

	# ==========================
	# HELPER FUNCTIONS
	# ==========================
	@staticmethod
	def try_float(value):
		try:
			return float(value)
		except (TypeError, ValueError):
			return None

	@staticmethod
	def try_int(value):
		try:
			return int(float(value))
		except (TypeError, ValueError):
			return None

	def _get_connection(self):
		db_path = self.db_file or 
				  os.path.abspath(os.path.join(os.path.dirname(__file__), '../../db/local.db'))
		conn = sqlite3.connect(db_path)
		return conn, conn.cursor()

	def _log_insert(self, dataset_name, data):
		logging.info(f"Inserted {len(data)} records into {dataset_name}")

    def insert_data(self, dataset_key: str, data: list[dict]) -> None:
        method = getattr(self, f'insert_{dataset_key}', None)
        if not method:
            logging.warning(f"No insert method for {dataset_key}")
            return
        method(data)

	# ==========================
	# DATASET INSERT FUNCTIONS
	# ==========================
	def insert_parks_events(self, data: list[dict]) -> None:
		"""Insert park events into an SQLite database."""
        conn, cur = self._get_connection()

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

		for row in data:
			raw_date = row.get("date_and_time", "")
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
				row.get("event_name", "N/A"),
				row.get("location", "N/A"),
				parsed_date,
				row.get("borough", "N/A"),
				row.get("locationtype", "N/A"),
				row.get("group_name_partner", "N/A"),
				row.get("event_type", "N/A"),
				row.get("category", "N/A"),
				self.try_int(row.get("attendance", 0)),
				row.get("audience", "N/A"),
				row.get("source", "N/A")
			))
		_log_insert("nyc_parks_events", data)
    
		conn.commit()
    	cur.close()
    	conn.close()
	
	def insert_permitted_events_historical(self, data: list[dict]) -> None:
		"""Insert permitted events (historical) into SQLite database"""
        conn, cur = self._get_connection()

		cur.execute("""
			CREATE TABLE IF NOT EXISTS permitted_events (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				event_id INTEGER,
				event_name TEXT,
				start_date_time TEXT,
				end_date_time TEXT,
				event_agency TEXT,
				event_type TEXT,
				event_borough TEXT,
				event_location TEXT,
				event_street_side TEXT,
				street_closure_type TEXT,
				community_board TEXT,
				police_precinct TEXT,
				UNIQUE(event_id)
			);
		""")

		for row in data:
			cur.execute("""
				INSERT OR IGNORE INTO permitted_events (
					event_id, event_name, start_date_time, end_date_time,
					event_agency, event_type, event_borough, event_location,
					event_street_side, street_closure_type, community_board, police_precinct
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			""", (
				row.get("event_id"),
				row.get("event_name", "N/A"),
				row.get("start_date_time", "N/A"),
				row.get("end_date_time", "N/A"),
				row.get("event_agency", "N/A"),
				row.get("event_type", "N/A"),
				row.get("event_borough", "N/A"),
				row.get("event_location", "N/A"),
				row.get("event_street_side", "N/A"),
				row.get("street_closure_type", "N/A"),
				row.get("community_board", "N/A"),
				row.get("police_precinct", "N/A")
			))
		_log_insert("nyc_permitted_events_historical", data)
    
        conn.commit()
        cur.close()
        conn.close()
    
	def insert_permitted_events_future(self, data: list[dict]) -> None:
		"""Insert real-time (1 mo) permitted events into SQLite database."""
        conn, cur = self._get_connection()

		cur.execute("""
			CREATE TABLE IF NOT EXISTS permitted_events_realtime (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				event_id INTEGER,
				event_name TEXT,
				start_date_time TEXT,
				end_date_time TEXT,
				event_agency TEXT,
				event_type TEXT,
				event_borough TEXT,
				event_location TEXT,
				event_street_side TEXT,
				street_closure_type TEXT,
				community_board TEXT,
				police_precinct TEXT,
				UNIQUE(event_id)
			);
		""")

		for row in data:
			cur.execute("""
				INSERT OR IGNORE INTO permitted_events_realtime (
					event_id, event_name, start_date_time, end_date_time,
					event_agency, event_type, event_borough, event_location,
					event_street_side, street_closure_type, community_board, police_precinct
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			""", (
				row.get("event_id"),
				row.get("event_name", "N/A"),
				row.get("start_date_time", "N/A"),
				row.get("end_date_time", "N/A"),
				row.get("event_agency", "N/A"),
				row.get("event_type", "N/A"),
				row.get("event_borough", "N/A"),
				row.get("event_location", "N/A"),
				row.get("event_street_side", "N/A"),
				row.get("street_closure_type", "N/A"),
				row.get("community_board", "N/A"),
				row.get("police_precinct", "N/A")
			))
		_log_insert("nyc_permitted_events_future", data)

        conn.commit()
        cur.close()
        conn.close()

	def insert_311_requests(self, data: list[dict]) -> None:
		"""Insert 311 requests into SQLite database"""
        conn, cur = self._get_connection()

		cur.execute("""
			CREATE TABLE IF NOT EXISTS nyc_311_requests (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				unique_key TEXT UNIQUE,
				created_date TEXT,
				closed_date TEXT,
				agency TEXT,
				agency_name TEXT,
				complaint_type TEXT,
				descriptor TEXT,
				location_type TEXT,
				incident_zip TEXT,
				incident_address TEXT,
				street_name TEXT,
				city TEXT,
				status TEXT,
				due_date TEXT,
				resolution_description TEXT,
				resolution_action_updated_date TEXT,
				borough TEXT,
				latitude REAL,
				longitude REAL
			);
		""")

		for row in data:
			cur.execute("""
				INSERT OR IGNORE INTO nyc_311_requests (
					unique_key, created_date, closed_date, agency, agency_name,
					complaint_type, descriptor, location_type, incident_zip,
					incident_address, street_name, city, status, due_date,
					resolution_description, resolution_action_updated_date,
					borough, latitude, longitude
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			""", (
				row.get("unique_key"),
				row.get("created_date", "N/A"),
				row.get("closed_date", "N/A"),
				row.get("agency", "N/A"),
				row.get("agency_name", "N/A"),
				row.get("complaint_type", "N/A"),
				row.get("descriptor", "N/A"),
				row.get("location_type", "N/A"),
				row.get("incident_zip", "N/A"),
				row.get("incident_address", "N/A"),
				row.get("street_name", "N/A"),
				row.get("city", "N/A"),
				row.get("status", "N/A"),
				row.get("due_date", "N/A"),
				row.get("resolution_description", "N/A"),
				row.get("resolution_action_updated_date", "N/A"),
				row.get("borough", "N/A"),
				self.try_float(row.get("latitude", 0.0)),
				self.try_float(row.get("longitude", 0.0))
			))
		_log_insert("nyc_311_requests", data)

        conn.commit()
        cur.close()
        conn.close()

	def insert_311_resolutions(self, data: list[dict]) -> None:
		"""Insert 311 resolution responses into SQLite database"""
        conn, cur = self._get_connection()

		cur.execute("""
			CREATE TABLE IF NOT EXISTS nyc_311_resolutions (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				unique_key TEXT UNIQUE,
				agency TEXT,
				agency_name TEXT,
				complaint_type TEXT,
				descriptor TEXT,
				borough TEXT,
				resolution_description TEXT,
				year INTEGER,
				month INTEGER,
				overall_satisfaction TEXT,
				dissatisfaction_reason TEXT
			);
		""")

		for row in data:
			cur.execute("""
				INSERT OR IGNORE INTO nyc_311_resolutions (
					unique_key, agency, agency_name, complaint_type, descriptor,
					borough, resolution_description, year, month,
					overall_satisfaction, dissatisfaction_reason
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			""", (
				row.get("unique_key"),
				row.get("agency", "N/A"),
				row.get("agency_name", "N/A"),
				row.get("complaint_type", "N/A"),
				row.get("descriptor", "N/A"),
				row.get("borough", "N/A"),
				row.get("resolution_description", "N/A"),
				self.try_int(row.get("year", 0)),
				self.try_int(row.get("month", 0)),
				row.get("overall_satisfaction", "N/A"),
				row.get("dissatisfaction_reason", "N/A")
			))
		_log_insert("nyc_311_resolutions", data)

        conn.commit()
        cur.close()
        conn.close()
		
	def insert_linknyc_status(self, data: list[dict]) -> None:
		"""Insert LinkNYC kiosk status into SQLite database"""
        conn, cur = self._get_connection()

		cur.execute("""
			CREATE TABLE IF NOT EXISTS linknyc_status (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				generated_on TEXT,
				site_id TEXT UNIQUE,
				status TEXT,
				kiosk_type TEXT,
				ppt_id TEXT,
				address TEXT,
				city TEXT,
				state TEXT,
				zip TEXT,
				boro TEXT,
				latitude REAL,
				longitude REAL,
				cross_street_1 TEXT,
				cross_street_2 TEXT,
				corner TEXT,
				community_board TEXT,
				council_district TEXT,
				census_tract TEXT,
				nta TEXT,
				bbl TEXT,
				bin TEXT,
				install_date TEXT,
				active_date TEXT,
				wifi_status TEXT,
				wifi_status_date TEXT,
				tablet_status TEXT,
				tablet_status_date TEXT,
				phone_status TEXT,
				phone_status_date TEXT
			);
		""")

		for row in data:
			cur.execute("""
				INSERT OR IGNORE INTO linknyc_status (
					generated_on, site_id, status, kiosk_type, ppt_id,
					address, city, state, zip, boro, latitude, longitude,
					cross_street_1, cross_street_2, corner, community_board,
					council_district, census_tract, nta, bbl, bin, install_date,
					active_date, wifi_status, wifi_status_date, tablet_status,
					tablet_status_date, phone_status, phone_status_date
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
						  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			""", (
				row.get("generated_on"),
				row.get("site_id"),
				row.get("status"),
				row.get("kiosk_type"),
				row.get("ppt_id"),
				row.get("address"),
				row.get("city"),
				row.get("state"),
				row.get("zip"),
				row.get("boro"),
				self.try_float(row.get("latitude")),
				self.try_float(row.get("longitude")),
				row.get("cross_street_1"),
				row.get("cross_street_2"),
				row.get("corner"),
				row.get("community_board"),
				row.get("council_district"),
				row.get("census_tract"),
				row.get("nta"),
				row.get("bbl"),
				row.get("bin"),
				row.get("install_date"),
				row.get("active_date"),
				row.get("wifi_status"),
				row.get("wifi_status_date"),
				row.get("tablet_status"),
				row.get("tablet_status_date"),
				row.get("phone_status"),
				row.get("phone_status_date")
			))
		_log_insert("linknyc_kiosk_status", data)

        conn.commit()
        cur.close()
        conn.close()

	def insert_sidewalk_status(self, data: list[dict]) -> None:
		"""Insert sidewalk status into SQLite database"""
        conn, cur = self._get_connection()

		cur.execute("""
			CREATE TABLE IF NOT EXISTS sidewalk_status (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				broken TEXT,
				cb INTEGER,
				certi_date TEXT,
				contract TEXT,
				entrydate TEXT,
				flag TEXT,
				frstname TEXT,
				grace_pd INTEGER,
				hardware TEXT,
				house_num TEXT,
				integrity TEXT,
				onfrtocode TEXT,
				onstname TEXT,
				other_def TEXT,
				patchwork TEXT,
				post_date TEXT,
				slope TEXT,
				sq_feet INTEGER,
				sw_missing TEXT,
				swv_number INTEGER,
				tostname TEXT,
				trip_haz TEXT,
				undermined TEXT,
				vdismissdate TEXT,
				violationid INTEGER,
				vissuedate TEXT,
				bblid INTEGER
			);
		""")

		for row in data:
			cur.execute("""
				INSERT OR IGNORE INTO sidewalk_status (
					broken, cb, certi_date, contract, entrydate, flag, frstname,
					grace_pd, hardware, house_num, integrity, onfrtocode, onstname,
					other_def, patchwork, post_date, slope, sq_feet, sw_missing,
					swv_number, tostname, trip_haz, undermined, vdismissdate,
					violationid, vissuedate, bblid
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
						  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			""", (
				row.get("broken"),
				try_int(row.get("cb")),
				row.get("certi_date"),
				row.get("contract"),
				row.get("entrydate"),
				row.get("flag"),
				row.get("frstname"),
				self.try_int(row.get("grace_pd")),
				row.get("hardware"),
				row.get("house_num"),
				row.get("integrity"),
				row.get("onfrtocode"),
				row.get("onstname"),
				row.get("other_def"),
				row.get("patchwork"),
				row.get("post_date"),
				row.get("slope"),
				self.try_int(row.get("sq_feet")),
				row.get("sw_missing"),
				self.try_int(row.get("swv_number")),
				row.get("tostname"),
				row.get("trip_haz"),
				row.get("undermined"),
				row.get("vdismissdate"),
				self.try_int(row.get("violationid")),
				row.get("vissuedate"),
				self.try_int(row.get("bblid"))
			))
		_log_insert("nyc_sidewalk_status", data)

        conn.commit()
        cur.close()
        conn.close()

	def insert_tree_points(self, data: list[dict]) -> None:
		"""Insert tree point into SQLite database"""
        conn, cur = self._get_connection()

		cur.execute("""
			CREATE TABLE IF NOT EXISTS tree_points (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				objectid INTEGER,
				dbh INTEGER,
				tpstructure TEXT,
				tpcondition TEXT,
				stumpdiameter TEXT,
				plantingspaceglobalid TEXT,
				geometry TEXT,
				globalid TEXT,
				genusspecies TEXT,
				createddate TEXT,
				updateddate TEXT,
				planteddate TEXT,
				riskrating TEXT,
				riskratingdate TEXT,
				location TEXT
			);
		""")

		for row in data:
			cur.execute("""
				INSERT OR IGNORE INTO tree_points (
					objectid, dbh, tpstructure, tpcondition, stumpdiameter,
					plantingspaceglobalid, geometry, globalid, genusspecies,
					createddate, updateddate, planteddate, riskrating,
					riskratingdate, location
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			""", (
				self.try_int(row.get("objectid")),
				self.try_int(row.get("dbh")),
				row.get("tpstructure"),
				row.get("tpcondition"),
				row.get("stumpdiameter"),
				row.get("plantingspaceglobalid"),
				row.get("geometry"),
				row.get("globalid"),
				row.get("genusspecies"),
				row.get("createddate"),
				row.get("updateddate"),
				row.get("planteddate"),
				row.get("riskrating"),
				row.get("riskratingdate"),
				row.get("location")
			))
		_log_insert("nyc_tree_points", data)

        conn.commit()
        cur.close()
        conn.close()
