import os
import sqlite3
import time
import logging
from dateutil import parser as date_parser
from typing import List, Dict
import re

__all__ = ["InsertManager"]

def load_table_schemas_from_file(sql_file_path: str) -> dict:
    with open(sql_file_path, "r") as f:
        sql_content = f.read()

    # Split based on CREATE TABLE statements
    statements = re.findall(r'CREATE TABLE IF NOT EXISTS (\w+)\s*\((.*?)\);', 
                            sql_content, 
                            re.DOTALL
                    )

    table_schemas = {}
    for table_name, schema_body in statements:
        full_statement = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_body});"
        table_schemas[table_name] = full_statement

    return table_schemas

class InsertManager:
    def __init__(self, db_file, schema_path='db/schema.sql'):
        self.db_file = db_file
        self.TABLE_SCHEMAS = load_table_schemas_from_file(schema_path)

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

    # ==========================
    # INSTANCE METHODS & CLASS UTILITIES
    # ==========================
    def _get_connection(self):
        """Opens a connection to the SQLite database and 
        provide a cursor for executing SQL commands."""
        db_path = self.db_file or os.path.abspath(os.path.join(os.path.dirname(__file__), '../../db/local.db'))
        conn = sqlite3.connect(db_path)
        return conn, conn.cursor()

    @staticmethod
    def initialize_database(db_file: str, schema_path: str):
        """Class-level utility to initialize the entire database schema (run once)."""   
        with sqlite3.connect(db_file) as conn:
            with open(schema_path, "r") as f:
                schema_sql = f.read()   
            conn.executescript(schema_sql)
        logging.info(f"Database schema initialized at {db_file}")

    def table_exists(self, dataset_name: str) -> bool:
        """Instance method to check for the existence of a specific table."""
        conn, cur = self._get_connection()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (dataset_name,)
        )
        exists = cur.fetchone() is not None
        cur.close()
        conn.close()
        return exists

    def insert_generic(self, dataset_name, schema_sql, insert_sql, row_mapper, data):
        """Core instance method for inserting data with any schema."""
        conn, cur = self._get_connection()
        cur.execute(schema_sql) # Ensure table exists 
        
        changes_before = conn.total_changes
   
        for row in data:
            values = row_mapper(row)
            cur.execute(insert_sql, values)
        
        conn.commit()

        changes_after = conn.total_changes
        inserted_count = changes_after - changes_before

        logging.info(f"Attempted {len(data)} inserts. " 
                     f"Actually inserted {inserted_count} new rows into {dataset_name}.")
        
        cur.close()
        conn.close()

    # ==========================
    # DATASET INSERT METHODS
    # ==========================
    def insert_parks_events(self, data: List[Dict]) -> None:
        """Insert park events into an SQLite database."""
        dataset_name = "nyc_parks_events"
        insert_sql = f"""
            INSERT OR IGNORE INTO {dataset_name} (
                event_name, location, date_and_time, borough,
                location_type, group_name_partner, event_type,
                category, attendance, audience, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        def row_mapper(row):
            try:
                dt = date_parser.parse(row.get("date_and_time", "")).isoformat(sep=' ')
            except (ValueError, TypeError):
                dt = None
            return (
                row.get("event_name", "N/A"),
                row.get("location", "N/A"),
                dt,
                row.get("borough", "N/A"),
                row.get("location_type", "N/A"),
                row.get("group_name_partner", "N/A"),
                row.get("event_type", "N/A"),
                row.get("category", "N/A"),
                self.try_int(row.get("attendance", 0)),
                row.get("audience", "N/A"),
                row.get("source", "N/A")
            )

        return self.insert_generic(
            dataset_name,
            self.TABLE_SCHEMAS[dataset_name],
            insert_sql,
            row_mapper,
            data
        )
    
    def insert_permitted_events_historical(self, data: List[Dict]) -> None:
        """Insert permitted events (historical) into SQLite database"""
        dataset_name = "nyc_permitted_events_historical"
        insert_sql = f"""
                INSERT OR IGNORE INTO {dataset_name} (
                    event_id, event_name, start_date_time, end_date_time,
                    event_agency, event_type, event_borough, event_location,
                    event_street_side, street_closure_type, community_board, police_precinct
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    
        def row_mapper(row):
            # Convert dict row to tuple of values in order expected by insert_sql       
            return (
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
            )

        return self.insert_generic(
            dataset_name,
            self.TABLE_SCHEMAS[dataset_name],
            insert_sql,
            row_mapper,
            data
        )
    
    def insert_permitted_events_future(self, data: List[Dict]) -> None:
        """Insert real-time (1 mo) permitted events into SQLite database."""
        dataset_name = "nyc_permitted_events_future"
        insert_sql = f"""
                INSERT OR IGNORE INTO {dataset_name} (
                    event_id, event_name, start_date_time, end_date_time,
                    event_agency, event_type, event_borough, event_location,
                    event_street_side, street_closure_type, community_board, police_precinct
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    
        def row_mapper(row):
            # Convert dict row to tuple of values in order expected by insert_sql       
            return (
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
            )

        return self.insert_generic(
            dataset_name,
            self.TABLE_SCHEMAS[dataset_name],
            insert_sql,
            row_mapper,
            data
        )

    def insert_311_requests(self, data: List[Dict]) -> None:
        """Insert 311 requests into SQLite database"""
        dataset_name = "nyc_311_requests"
        insert_sql = f"""
                INSERT OR IGNORE INTO {dataset_name} (
                    unique_key, created_date, closed_date, agency, agency_name,
                    complaint_type, descriptor, location_type, incident_zip,
                    incident_address, street_name, city, status, due_date,
                    resolution_description, resolution_action_updated_date,
                    borough, latitude, longitude
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    
        def row_mapper(row):
            # Convert dict row to tuple of values in order expected by insert_sql       
            return (
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
            )

        return self.insert_generic(
            dataset_name,
            self.TABLE_SCHEMAS[dataset_name],
            insert_sql,
            row_mapper,
            data
        )

    def insert_311_resolutions(self, data: List[Dict]) -> None:
        """Insert 311 resolution responses into SQLite database"""
        dataset_name = "nyc_311_resolutions"
        insert_sql = f"""
                INSERT OR IGNORE INTO {dataset_name} (
                    unique_key, agency, agency_name, complaint_type, descriptor,
                    borough, resolution_description, year, month,
                    overall_satisfaction, dissatisfaction_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    
        def row_mapper(row):
            # Convert dict row to tuple of values in order expected by insert_sql       
            return (
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
            )

        return self.insert_generic(
            dataset_name,
            self.TABLE_SCHEMAS[dataset_name],
            insert_sql,
            row_mapper,
            data
        )
        
    def insert_linknyc_status(self, data: List[Dict]) -> None:
        """Insert LinkNYC kiosk status into SQLite database"""
        dataset_name = "linknyc_status"
        insert_sql = f"""
                INSERT OR IGNORE INTO {dataset_name} (
                    generated_on, site_id, status, kiosk_type, ppt_id,
                    address, city, state, zip, boro, latitude, longitude,
                    cross_street_1, cross_street_2, corner, community_board,
                    council_district, census_tract, nta, bbl, bin, install_date,
                    active_date, wifi_status, wifi_status_date, tablet_status,
                    tablet_status_date, phone_status, phone_status_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    
        def row_mapper(row):
            # Convert dict row to tuple of values in order expected by insert_sql       
            return (
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
            )

        return self.insert_generic(
            dataset_name,
            self.TABLE_SCHEMAS[dataset_name],
            insert_sql,
            row_mapper,
            data
        )

    def insert_sidewalk_status(self, data: List[Dict]) -> None:
        """Insert sidewalk status into SQLite database"""
        dataset_name = "nyc_sidewalk_status"
        insert_sql = f"""
                INSERT OR IGNORE INTO {dataset_name} (
                    broken, cb, certi_date, contract, entrydate, flag, frstname,
                    grace_pd, hardware, house_num, integrity, onfrtocode, onstname,
                    other_def, patchwork, post_date, slope, sq_feet, sw_missing,
                    swv_number, tostname, trip_haz, undermined, vdismissdate,
                    violationid, vissuedate, bblid
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    
        def row_mapper(row):
            # Convert dict row to tuple of values in order expected by insert_sql       
            return (
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
            )

        return self.insert_generic(
            dataset_name,
            self.TABLE_SCHEMAS[dataset_name],
            insert_sql,
            row_mapper,
            data
        )

    def insert_tree_points(self, data: List[Dict]) -> None:
        """Insert tree point into SQLite database"""
        dataset_name = "nyc_tree_points"
        insert_sql = f"""
                INSERT OR IGNORE INTO {dataset_name} (
                    objectid, dbh, tpstructure, tpcondition, stumpdiameter,
                    plantingspaceglobalid, geometry, globalid, genusspecies,
                    createddate, updateddate, planteddate, riskrating,
                    riskratingdate, location
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    
        def row_mapper(row):
            # Convert dict row to tuple of values in order expected by insert_sql       
            return (
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
            )

        return self.insert_generic(
            dataset_name,
            self.TABLE_SCHEMAS[dataset_name],
            insert_sql,
            row_mapper,
            data
        )
