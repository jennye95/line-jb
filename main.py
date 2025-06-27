import os
import logging
# import streamlit as st
# from line_jb.data_ingestion.search import fetch_posts_by_hashtag
from line_jb.data_ingestion.insert_manager import InsertManager
from line_jb.data_ingestion.fetch_nyc_open_data import fetch_nyc_data
from line_jb.geospatial.geo_processor import GeoProcessor
from line_jb.geospatial.map_renderer import MapRenderer   
from line_jb.geospatial.osm_utils import OSMUtils         

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
    geo_processor = GeoProcessor(db_path) # Initialize GeoProcessor
    map_renderer = MapRenderer()         # Initialize MapRenderer
    osm_utils = OSMUtils()               # Initialize OSMUtils

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
            logging.info(f"Looking for method '{insert_method_name}' in InsertManager instance.")
            logging.info(f"Available insert methods: {[m for m in dir(inserter) if m.startswith('insert_')]}")
            logging.info(f"table_name repr: {repr(table_name)}")
            insert_func = getattr(inserter, insert_method_name)
            insert_func(data)

        except AttributeError:
            logging.error(f"Insert method '{insert_method_name}' not found. Check insert_manager.py for missing or misspelled methods.")
        except Exception as e:
            logging.error(f"Failed to process {dataset_key}: {e}")

    # --- Geospatial Processing and Mapping ---
    logging.info("Starting geospatial processing and map rendering.")

    # 1. Load data into GeoDataFrames
    parks_events_gdf = geo_processor.load_data_as_geodataframe("nyc_parks_events")
    permitted_events_historical_gdf = geo_processor.load_data_as_geodataframe("nyc_permitted_events_historical")
    permitted_events_future_gdf = geo_processor.load_data_as_geodataframe("nyc_permitted_events_future")
    _311_requests_gdf = geo_processor.load_data_as_geodataframe("nyc_311_requests")
    linknyc_status_gdf = geo_processor.load_data_as_geodataframe("linknyc_status")
    sidewalk_status_gdf = geo_processor.load_data_as_geodataframe("nyc_sidewalk_status", lat_col='latitude', lon_col='longitude') # Assuming lat/lon exist
    tree_points_gdf = geo_processor.load_data_as_geodataframe("nyc_tree_points", lat_col='latitude', lon_col='longitude') # Assuming lat/lon exist

    # 2. Fetch OSM data for relevant areas (e.g., NYC parks for prioritization)
    # You'll need to define the 'query' or 'place_name' for fetching specific OSM features.
    # For NYC Parks, you might fetch parks by a known boundary or by a general NYC query with specific tags.
    nyc_parks_osm_gdf = osm_utils.get_osm_features(query="New York City, New York, USA", tags={"leisure": "park"}, gdf_type='polygons')

    # 3. Calculate historical event density for prioritization
    # This assumes you can map events to parks in nyc_parks_osm_gdf
    if not nyc_parks_osm_gdf.empty and not permitted_events_historical_gdf.empty:
        parks_with_event_counts = geo_processor.calculate_historical_event_density(
            parks_gdf=nyc_parks_osm_gdf.copy(), # Work on a copy
            events_gdf=permitted_events_historical_gdf
        )
        # Example of how to style based on event count
        def park_style(feature):
            event_count = feature['properties'].get('event_count', 0)
            if event_count > 50:
                return {'fillColor': 'darkgreen', 'color': 'darkgreen', 'weight': 2, 'fillOpacity': 0.8}
            elif event_count > 10:
                return {'fillColor': 'green', 'color': 'green', 'weight': 1.5, 'fillOpacity': 0.6}
            else:
                return {'fillColor': 'lightgreen', 'color': 'lightgreen', 'weight': 1, 'fillOpacity': 0.4}

        map_renderer.add_geodataframe_layer(parks_with_event_counts, 
                                            name="NYC Parks (Prioritized)", 
                                            style_function=park_style,
                                            popup_fields=['name', 'event_count'])

    # 4. Add other layers to the map
    map_renderer.add_geodataframe_layer(_311_requests_gdf, name="311 Requests", color='red', 
                                        popup_fields=['complaint_type', 'status', 'created_date'])
    map_renderer.add_geodataframe_layer(linknyc_status_gdf, name="LinkNYC Kiosks", color='purple', marker_type='circle_marker',
                                        popup_fields=['status', 'kiosk_type', 'address'])
    map_renderer.add_geodataframe_layer(permitted_events_future_gdf, name="Future Permitted Events", color='blue', marker_type='circle_marker',
                                        popup_fields=['event_name', 'start_date_time', 'event_type'])

    # Add a layer control to toggle layers
    folium.LayerControl().add_to(map_renderer.get_map_object())

    # 5. Save the map (or integrate with a web framework for dynamic serving)
    map_renderer.save_map("nyc_data_map.html")
    logging.info("Map generation complete. Check nyc_data_map.html")

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
