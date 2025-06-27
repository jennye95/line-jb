import os
import logging
# import streamlit as st
# from line_jb.data_ingestion.search import fetch_posts_by_hashtag
import folium # Import folium for LayerControl
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

    # 1. Load data with explicit Latitude/Longitude into GeoDataFrames
    # These are the datasets confirmed to have lat/lon directly:
    _311_requests_gdf = geo_processor.load_data_as_geodataframe("nyc_311_requests", lat_col='latitude', lon_col='longitude')
    linknyc_status_gdf = geo_processor.load_data_as_geodataframe("linknyc_status", lat_col='latitude', lon_col='longitude')

    # nyc_parks_events, nyc_sidewalk_status, nyc_tree_points, nyc_permitted_events_historical, nyc_permitted_events_future  are ingested,
    # but not loaded as GeoDataFrames here as they don't have direct lat/lon for point plotting.
    # You would need alternative methods for these, e.g., parsing geometry fields or joining with other geo data.

    # 2. Fetch OSM data for relevant areas (e.g., NYC parks for prioritization)
    # Using a place name for a general query to get park polygons.
    # You might need to refine this query to get accurate park boundaries for NYC.
    nyc_parks_osm_gdf = osm_utils.get_osm_features(
        query="New York City, New York, USA",
        tags={"leisure": "park", "landuse": "park", "boundary": "national_park"}, # Common tags for parks
        gdf_type='polygons'
    )

    # 3. Calculate historical event density for prioritization
    # This now explicitly uses the 'permitted_events_historical_gdf' as the source of event points.
    if not nyc_parks_osm_gdf.empty and not permitted_events_historical_gdf.empty:
        parks_with_event_counts = geo_processor.calculate_historical_event_density(
            parks_gdf=nyc_parks_osm_gdf.copy(), # Pass a copy to avoid modifying original
            events_gdf=permitted_events_historical_gdf
        )
        
        # Define a style function for parks based on event count
        def park_priority_style(feature):
            event_count = feature['properties'].get('event_count', 0)
            if event_count >= 10: # Example threshold for "stand out"
                return {'fillColor': '#006400', 'color': '#003300', 'weight': 2, 'fillOpacity': 0.8} # Darker green
            elif event_count >= 3:
                return {'fillColor': '#32CD32', 'color': '#008000', 'weight': 1.5, 'fillOpacity': 0.6} # Medium green
            else:
                return {'fillColor': '#90EE90', 'color': '#6B8E23', 'weight': 1, 'fillOpacity': 0.4} # Lighter green
        
        # Add the prioritized parks layer
        map_renderer.add_geodataframe_layer(
            parks_with_event_counts,
            name="NYC Parks (Prioritized by Historical Events)",
            style_function=park_priority_style,
            popup_fields=['name', 'event_count'] # Assuming 'name' exists in OSM park data
        )
    else:
        logging.warning("Skipping park prioritization layer: OSM parks or historical events data is empty.")

    # 4. Add other layers to the map
    map_renderer.add_geodataframe_layer(
        _311_requests_gdf,
        name="311 Service Requests",
        color='red',
        popup_fields=['complaint_type', 'status', 'created_date', 'borough']
    )
    map_renderer.add_geodataframe_layer(
        linknyc_status_gdf,
        name="LinkNYC Kiosks",
        color='purple',
        marker_type='circle_marker', # Specify circle marker for points
        popup_fields=['status', 'kiosk_type', 'address', 'wifi_status']
    )
    map_renderer.add_geodataframe_layer(
        permitted_events_future_gdf,
        name="Future Permitted Events",
        color='orange', # Changed color for distinction
        marker_type='circle_marker', # Specify circle marker for points
        popup_fields=['event_name', 'start_date_time', 'event_type', 'event_borough']
    )
    
    # Add a layer control so users can toggle layers on/off
    folium.LayerControl().add_to(map_renderer.get_map_object())

    # 5. Save the map to an HTML file
    map_renderer.save_map("nyc_data_map.html")
    logging.info("Map generation complete. Open nyc_data_map.html in your browser.")    

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
