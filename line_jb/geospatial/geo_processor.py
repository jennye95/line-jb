import geopandas
import pandas as pd
import sqlite3
from shapely.geometry import Point
import logging

logging.basicConfig(level=logging.INFO)

class GeoProcessor:
    def __init__(self, db_path):
        self.db_path = db_path

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def load_data_as_geodataframe(self, table_name, lat_col='latitude', lon_col='longitude'):
        """
        Loads a table from the database into a GeoDataFrame,
        assuming latitude and longitude columns exist.
        """
        try:
            conn = self._get_connection()
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            conn.close()

            # Drop rows where lat/lon are missing before creating points
            df = df.dropna(subset=[lat_col, lon_col])

            # Create Point geometries from latitude and longitude
            geometry = [Point(xy) for xy in zip(df[lon_col], df[lat_col])]
            gdf = geopandas.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
            logging.info(f"Loaded {len(gdf)} records from {table_name} into GeoDataFrame.")
            return gdf
        except Exception as e:
            logging.error(f"Error loading {table_name} into GeoDataFrame: {e}")
            return geopandas.GeoDataFrame() # Return empty GeoDataFrame on error

    def get_data_in_bbox(self, geodataframe, min_lon, min_lat, max_lon, max_lat):
        """
        Filters a GeoDataFrame to return features within a given bounding box.
        Bounding box format: (min_lon, min_lat, max_lon, max_lat)
        """
        bbox_polygon = geopandas.GeoSeries([geopandas.geometry.box(min_lon, min_lat, max_lon, max_lat)], crs="EPSG:4326")
        return geodataframe[geodataframe.geometry.within(bbox_polygon.unary_union)]

    def calculate_historical_event_density(self, parks_gdf, events_gdf):
        """
        Calculates event density for parks.
        Requires GeoDataFrames for parks (with park geometries) and historical events (points).
        """
        # This is a conceptual example. Actual implementation depends on how you define 
        # "parks" geometry and how you want to aggregate.
        # For simplicity, let's assume 'parks_gdf' has polygon geometries for parks.

        # Perform a spatial join to count events within each park
        if not parks_gdf.empty and not events_gdf.empty:
            # Ensure CRSs match before spatial join
            if parks_gdf.crs != events_gdf.crs:
                events_gdf = events_gdf.to_crs(parks_gdf.crs)

            # Use sjoin to find events within parks
            events_in_parks = geopandas.sjoin(events_gdf, parks_gdf, how="inner", predicate="within")

            # Count events per park and merge back to parks_gdf
            event_counts = events_in_parks.groupby('index_right').size().rename('event_count')
            parks_gdf_with_counts = parks_gdf.join(event_counts)
            parks_gdf_with_counts['event_count'] = parks_gdf_with_counts['event_count'].fillna(0).astype(int)
            logging.info("Calculated historical event density for parks.")
            return parks_gdf_with_counts
        logging.warning("Skipping historical event density calculation due to empty GeoDataFrames.")
        return parks_gdf # Return original if inputs are empty
