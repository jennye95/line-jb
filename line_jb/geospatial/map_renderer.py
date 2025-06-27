import folium
import geopandas as gpd
import logging

logging.basicConfig(level=logging.INFO)

class MapRenderer:
    def __init__(self, location=(40.7128, -74.0060), zoom_start=12): # Default to NYC
        self.map = folium.Map(location=location, zoom_start=zoom_start)

    def add_geodataframe_layer(self, gdf, name, color='blue', popup_fields=None, style_function=None, marker_type='circle_marker'):
        """
        Adds a GeoDataFrame as a layer to the map.
        """
        if gdf.empty:
            logging.warning(f"Skipping empty GeoDataFrame for layer: {name}")
            return

        if style_function is None:
            # Default style function for polygons or lines
            style_function = lambda x: {
                'fillColor': color,
                'color': color,
                'weight': 1,
                'fillOpacity': 0.7
            }

        # Prepare pop-up content
        tooltip_fields = popup_fields if popup_fields else gdf.columns.tolist() # Show all fields if none specified

        if marker_type == 'circle_marker' and gdf.crs.name == 'WGS 84' and all(gdf.geometry.type == 'Point'):
            # Handle point data with CircleMarkers
            for idx, row in gdf.iterrows():
                popup_html = "<table>"
                for field in tooltip_fields:
                    popup_html += f"<tr><td><b>{field}:</b></td><td>{row.get(field, 'N/A')}</td></tr>"
                popup_html += "</table>"

                folium.CircleMarker(
                    location=[row.geometry.y, row.geometry.x],
                    radius=5,
                    color=color,
                    fill=True,
                    fill_color=color,
                    fill_opacity=0.7,
                    tooltip=folium.Tooltip(row.get(tooltip_fields[0], 'Detail')), # First field as tooltip
                    popup=folium.Popup(popup_html, max_width=300)
                ).add_to(self.map)
        else:
            # Use GeoJson for polygons, lines, or if not specified as circle_marker
            folium.GeoJson(
                gdf.to_json(),
                name=name,
                style_function=style_function,
                tooltip=folium.features.GeoJsonTooltip(fields=tooltip_fields)
            ).add_to(self.map)

        logging.info(f"Added '{name}' layer to the map.")


    def save_map(self, filename="map.html"):
        """Saves the map to an HTML file."""
        try:
            self.map.save(filename)
            logging.info(f"Map saved to {filename}")
        except Exception as e:
            logging.error(f"Error saving map: {e}")

    def get_map_object(self):
        """Returns the Folium map object for further manipulation."""
        return self.map
