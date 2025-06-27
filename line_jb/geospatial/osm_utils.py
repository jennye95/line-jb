import osmnx as ox
import geopandas as gpd
import logging

logging.basicConfig(level=logging.INFO)

class OSMUtils:
    def __init__(self):
        # Configure OSMnx if needed, e.g., cache folder
        ox.config(use_cache=True, log_console=False)

    def get_street_network(self, place_name=None, bbox=None):
        """
        Fetches street network for a given place name or bounding box.
        Returns a MultiDiGraph (OSMnx graph).
        """
        try:
            if place_name:
                G = ox.graph_from_place(place_name, network_type="all")
                logging.info(f"Fetched street network for '{place_name}'.")
            elif bbox:
                north, south, east, west = bbox
                G = ox.graph_from_bbox(north, south, east, west, network_type="all")
                logging.info(f"Fetched street network for bbox: {bbox}.")
            else:
                raise ValueError("Must provide either 'place_name' or 'bbox'.")
            return G
        except Exception as e:
            logging.error(f"Error fetching street network: {e}")
            return None

    def get_osm_features(self, query, tags, gdf_type='points'):
        """
        Fetches specific OSM features (e.g., parks, buildings) by query and tags.
        Returns a GeoDataFrame.
        gdf_type can be 'points', 'polygons', 'lines'.
        """
        try:
            if gdf_type == 'points':
                gdf = ox.features_from_place(query, tags)
            elif gdf_type == 'polygons':
                gdf = ox.features_from_place(query, tags) # OSMnx generally returns polygons for areas
            else:
                gdf = ox.features_from_place(query, tags) # Handle lines generally

            logging.info(f"Fetched {len(gdf)} '{tags}' features for '{query}'.")
            return gdf
        except Exception as e:
            logging.error(f"Error fetching OSM features for '{query}' with tags {tags}: {e}")
            return gpd.GeoDataFrame() # Return empty GeoDataFrame on error
