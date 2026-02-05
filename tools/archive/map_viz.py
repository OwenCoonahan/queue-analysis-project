#!/usr/bin/env python3
"""
Geographic Map Visualization for Queue Analysis

Creates interactive maps showing POI locations with project data.

Usage:
    from map_viz import create_poi_map, create_congestion_heatmap

    # Create a map of projects
    map_html = create_poi_map(projects_df)

    # Save to file
    with open('poi_map.html', 'w') as f:
        f.write(map_html)
"""

import folium
from folium import plugins
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import json

# Output directory
MAP_DIR = Path(__file__).parent / 'maps'
MAP_DIR.mkdir(exist_ok=True)

# NY coordinates for NYISO region
NY_CENTER = [42.9, -75.5]
NY_ZOOM = 7

# Color schemes
RECOMMENDATION_COLORS = {
    'GO': '#22c55e',
    'CONDITIONAL': '#f59e0b',
    'NO-GO': '#ef4444',
    'Unknown': '#6b7280'
}

TYPE_COLORS = {
    'Solar': '#f59e0b',
    'Wind': '#0ea5e9',
    'Storage': '#10b981',
    'Hybrid': '#8b5cf6',
    'Gas': '#6b7280',
    'Load': '#ef4444',
    'L': '#f59e0b',  # NYISO codes
    'W': '#0ea5e9',
    'S': '#10b981',
    'H': '#8b5cf6',
}

# Known POI coordinates (NYISO substations/POIs)
# These would ideally come from a database, but here are some major ones
POI_COORDINATES = {
    'Marcy': (43.1726, -75.2896),
    'Fraser': (43.0832, -75.2896),
    'Edic': (43.0832, -75.3896),
    'Clay': (43.1726, -76.1896),
    'Volney': (43.3365, -76.3513),
    'Oswego': (43.4545, -76.5105),
    'Nine Mile Point': (43.5221, -76.4104),
    'Rotterdam': (42.7876, -73.9707),
    'Leeds': (42.2543, -73.8943),
    'Athens': (42.2609, -73.8096),
    'Pleasant Valley': (41.7437, -73.8207),
    'East Fishkill': (41.5437, -73.7896),
    'Millwood': (41.2043, -73.7943),
    'Sprain Brook': (41.0543, -73.8243),
    'Dunwoodie': (40.9376, -73.8676),
    'Rainey': (40.7643, -73.9443),
    'East 13th St': (40.7343, -73.9843),
    'Farragut': (40.6543, -73.9843),
    'Gowanus': (40.6743, -74.0043),
    'Corona': (40.7443, -73.8643),
    'Vernon': (40.7443, -73.9543),
    'Astoria': (40.7743, -73.9243),
    'Fresh Kills': (40.5743, -74.1843),
    'Goethals': (40.6343, -74.2043),
    'Linden': (40.6343, -74.2443),
    'Hudson Ave': (40.6943, -73.9843),
    'New Scotland': (42.6343, -73.8743),
    'Princetown': (42.8143, -74.0543),
    'Porter': (43.2443, -79.0343),
    'Niagara': (43.0943, -79.0543),
    'Gardenville': (42.8643, -78.7743),
    'Homer City': (40.5343, -79.1743),
    'Stolle Road': (42.8543, -78.6843),
    'Oakdale': (43.0443, -76.1443),
    'Pannell Road': (43.1543, -77.5543),
    'Station 80': (43.1343, -77.6043),
    'Mortimer': (42.9843, -77.4543),
    'Meyer': (43.1643, -77.6443),
}


def geocode_poi(poi_name: str) -> Optional[Tuple[float, float]]:
    """
    Get coordinates for a POI name.

    Args:
        poi_name: Name of the POI/substation

    Returns:
        Tuple of (lat, lon) or None if not found
    """
    # Direct match
    if poi_name in POI_COORDINATES:
        return POI_COORDINATES[poi_name]

    # Fuzzy match
    poi_lower = poi_name.lower()
    for known_poi, coords in POI_COORDINATES.items():
        if known_poi.lower() in poi_lower or poi_lower in known_poi.lower():
            return coords

    # Random jitter around NY center for unknown POIs
    return (
        NY_CENTER[0] + np.random.uniform(-2, 2),
        NY_CENTER[1] + np.random.uniform(-2, 2)
    )


def create_poi_map(
    projects_df: pd.DataFrame,
    color_by: str = 'recommendation',
    title: str = "Interconnection Queue Map",
    poi_column: str = 'poi',
    center: List[float] = None,
    zoom: int = None
) -> str:
    """
    Create an interactive map of projects by POI location.

    Args:
        projects_df: DataFrame with project data
        color_by: Column to use for marker colors ('recommendation', 'type', 'score')
        title: Map title
        poi_column: Column name containing POI names
        center: Map center [lat, lon]
        zoom: Initial zoom level

    Returns:
        HTML string of the map
    """
    center = center or NY_CENTER
    zoom = zoom or NY_ZOOM

    # Create base map
    m = folium.Map(
        location=center,
        zoom_start=zoom,
        tiles='cartodbpositron'
    )

    # Add title
    title_html = f'''
    <div style="position: fixed; top: 10px; left: 50px; z-index: 1000;
                background-color: white; padding: 10px 20px; border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1); font-family: sans-serif;">
        <h3 style="margin: 0; color: #1f2937;">{title}</h3>
        <p style="margin: 5px 0 0 0; font-size: 12px; color: #6b7280;">
            {len(projects_df)} projects
        </p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(title_html))

    # Prepare marker clusters by POI
    poi_groups = projects_df.groupby(poi_column) if poi_column in projects_df.columns else None

    if poi_groups is None:
        # No POI column, just plot all projects
        for _, row in projects_df.iterrows():
            coords = geocode_poi(str(row.get('name', 'Unknown')))
            if coords:
                _add_project_marker(m, row, coords, color_by)
    else:
        # Group by POI
        for poi_name, group in poi_groups:
            coords = geocode_poi(str(poi_name))
            if coords is None:
                continue

            # If multiple projects at same POI, create a cluster
            if len(group) > 1:
                # Create a marker cluster
                popup_html = _create_poi_popup(poi_name, group)

                # Determine cluster color based on best project
                if color_by == 'recommendation':
                    if 'GO' in group['recommendation'].values:
                        color = RECOMMENDATION_COLORS['GO']
                    elif 'CONDITIONAL' in group['recommendation'].values:
                        color = RECOMMENDATION_COLORS['CONDITIONAL']
                    else:
                        color = RECOMMENDATION_COLORS['NO-GO']
                elif color_by == 'score':
                    avg_score = group['score'].mean() if 'score' in group.columns else 50
                    color = '#22c55e' if avg_score >= 70 else ('#f59e0b' if avg_score >= 50 else '#ef4444')
                else:
                    # Type - use most common
                    most_common_type = group['type'].mode().iloc[0] if 'type' in group.columns else 'Unknown'
                    color = TYPE_COLORS.get(most_common_type, '#6b7280')

                folium.CircleMarker(
                    location=coords,
                    radius=8 + len(group) * 2,  # Size by project count
                    color=color,
                    fill=True,
                    fillColor=color,
                    fillOpacity=0.7,
                    popup=folium.Popup(popup_html, max_width=400),
                    tooltip=f"{poi_name}: {len(group)} projects"
                ).add_to(m)
            else:
                # Single project
                row = group.iloc[0]
                _add_project_marker(m, row, coords, color_by)

    # Add legend
    _add_legend(m, color_by)

    # Save and return
    map_path = MAP_DIR / 'poi_map.html'
    m.save(str(map_path))

    return m._repr_html_()


def _add_project_marker(
    m: folium.Map,
    row: pd.Series,
    coords: Tuple[float, float],
    color_by: str
) -> None:
    """Add a single project marker to the map."""
    # Determine color
    if color_by == 'recommendation':
        rec = row.get('recommendation', 'Unknown')
        color = RECOMMENDATION_COLORS.get(rec, '#6b7280')
    elif color_by == 'score':
        score = row.get('score', 50)
        color = '#22c55e' if score >= 70 else ('#f59e0b' if score >= 50 else '#ef4444')
    else:
        proj_type = row.get('type', 'Unknown')
        color = TYPE_COLORS.get(proj_type, '#6b7280')

    # Create popup
    popup_html = f"""
    <div style="font-family: sans-serif; min-width: 200px;">
        <h4 style="margin: 0 0 10px 0; color: #1f2937;">{row.get('name', 'Unknown')}</h4>
        <table style="font-size: 12px; width: 100%;">
            <tr><td style="color: #6b7280;">ID:</td><td><strong>{row.get('project_id', row.get('Queue ID', 'N/A'))}</strong></td></tr>
            <tr><td style="color: #6b7280;">Type:</td><td>{row.get('type', 'Unknown')}</td></tr>
            <tr><td style="color: #6b7280;">Capacity:</td><td>{row.get('capacity_mw', 0):,.0f} MW</td></tr>
            <tr><td style="color: #6b7280;">Developer:</td><td>{row.get('developer', 'Unknown')}</td></tr>
            <tr><td style="color: #6b7280;">Score:</td><td><strong>{row.get('score', 'N/A')}</strong></td></tr>
            <tr><td style="color: #6b7280;">Recommendation:</td><td style="color: {color}; font-weight: bold;">{row.get('recommendation', 'N/A')}</td></tr>
        </table>
    </div>
    """

    # Marker size based on capacity
    capacity = row.get('capacity_mw', 100)
    radius = min(max(5, capacity / 50), 20)

    folium.CircleMarker(
        location=coords,
        radius=radius,
        color=color,
        fill=True,
        fillColor=color,
        fillOpacity=0.7,
        popup=folium.Popup(popup_html, max_width=300),
        tooltip=f"{row.get('name', 'Unknown')} ({row.get('capacity_mw', 0):,.0f} MW)"
    ).add_to(m)


def _create_poi_popup(poi_name: str, group: pd.DataFrame) -> str:
    """Create popup HTML for a POI with multiple projects."""
    total_mw = group['capacity_mw'].sum() if 'capacity_mw' in group.columns else 0

    html = f"""
    <div style="font-family: sans-serif; min-width: 300px; max-height: 400px; overflow-y: auto;">
        <h4 style="margin: 0 0 10px 0; color: #1f2937; border-bottom: 2px solid #e5e7eb; padding-bottom: 8px;">
            {poi_name}
        </h4>
        <p style="margin: 0 0 10px 0; font-size: 12px; color: #6b7280;">
            {len(group)} projects | {total_mw:,.0f} MW total
        </p>
        <table style="font-size: 11px; width: 100%; border-collapse: collapse;">
            <tr style="background: #f3f4f6;">
                <th style="padding: 5px; text-align: left;">Project</th>
                <th style="padding: 5px; text-align: right;">MW</th>
                <th style="padding: 5px; text-align: center;">Score</th>
            </tr>
    """

    for _, row in group.iterrows():
        score = row.get('score', 'N/A')
        if isinstance(score, (int, float)):
            score_color = '#22c55e' if score >= 70 else ('#f59e0b' if score >= 50 else '#ef4444')
            score_display = f'<span style="color: {score_color}; font-weight: bold;">{score:.0f}</span>'
        else:
            score_display = 'N/A'

        html += f"""
            <tr style="border-bottom: 1px solid #e5e7eb;">
                <td style="padding: 5px;">{row.get('name', 'Unknown')[:30]}</td>
                <td style="padding: 5px; text-align: right;">{row.get('capacity_mw', 0):,.0f}</td>
                <td style="padding: 5px; text-align: center;">{score_display}</td>
            </tr>
        """

    html += "</table></div>"
    return html


def _add_legend(m: folium.Map, color_by: str) -> None:
    """Add a legend to the map."""
    if color_by == 'recommendation':
        items = [
            ('GO', RECOMMENDATION_COLORS['GO']),
            ('CONDITIONAL', RECOMMENDATION_COLORS['CONDITIONAL']),
            ('NO-GO', RECOMMENDATION_COLORS['NO-GO'])
        ]
        title = 'Recommendation'
    elif color_by == 'score':
        items = [
            ('70+ (GO)', '#22c55e'),
            ('50-69 (Conditional)', '#f59e0b'),
            ('<50 (No-Go)', '#ef4444')
        ]
        title = 'Score'
    else:
        items = [
            ('Solar', TYPE_COLORS.get('Solar', '#f59e0b')),
            ('Wind', TYPE_COLORS.get('Wind', '#0ea5e9')),
            ('Storage', TYPE_COLORS.get('Storage', '#10b981')),
            ('Hybrid', TYPE_COLORS.get('Hybrid', '#8b5cf6'))
        ]
        title = 'Project Type'

    legend_html = f'''
    <div style="position: fixed; bottom: 30px; right: 30px; z-index: 1000;
                background-color: white; padding: 15px; border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1); font-family: sans-serif;">
        <h4 style="margin: 0 0 10px 0; font-size: 12px; color: #1f2937;">{title}</h4>
    '''

    for label, color in items:
        legend_html += f'''
        <div style="margin: 5px 0;">
            <span style="display: inline-block; width: 12px; height: 12px;
                        background: {color}; border-radius: 50%; margin-right: 8px;"></span>
            <span style="font-size: 11px; color: #4b5563;">{label}</span>
        </div>
        '''

    legend_html += '</div>'
    m.get_root().html.add_child(folium.Element(legend_html))


def create_congestion_heatmap(
    projects_df: pd.DataFrame,
    poi_column: str = 'poi',
    weight_column: str = 'capacity_mw',
    title: str = "POI Congestion Heatmap"
) -> str:
    """
    Create a heatmap showing congestion at POIs.

    Args:
        projects_df: DataFrame with project data
        poi_column: Column name containing POI names
        weight_column: Column to use for heat intensity
        title: Map title

    Returns:
        HTML string of the map
    """
    m = folium.Map(
        location=NY_CENTER,
        zoom_start=NY_ZOOM,
        tiles='cartodbpositron'
    )

    # Prepare heatmap data
    heat_data = []

    if poi_column in projects_df.columns:
        poi_groups = projects_df.groupby(poi_column)
        for poi_name, group in poi_groups:
            coords = geocode_poi(str(poi_name))
            if coords:
                weight = group[weight_column].sum() if weight_column in group.columns else len(group)
                # Convert numpy types to native Python for JSON serialization
                heat_data.append([float(coords[0]), float(coords[1]), float(weight)])

    # Add heatmap layer
    if heat_data:
        plugins.HeatMap(
            heat_data,
            min_opacity=0.3,
            radius=25,
            blur=15,
            gradient={0.4: 'blue', 0.65: 'lime', 0.8: 'yellow', 1: 'red'}
        ).add_to(m)

    # Add title
    title_html = f'''
    <div style="position: fixed; top: 10px; left: 50px; z-index: 1000;
                background-color: white; padding: 10px 20px; border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1); font-family: sans-serif;">
        <h3 style="margin: 0; color: #1f2937;">{title}</h3>
        <p style="margin: 5px 0 0 0; font-size: 12px; color: #6b7280;">
            Heat intensity by {weight_column}
        </p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(title_html))

    # Save
    map_path = MAP_DIR / 'congestion_heatmap.html'
    m.save(str(map_path))

    return m._repr_html_()


# Demo
if __name__ == "__main__":
    # Create sample data
    sample_data = pd.DataFrame({
        'project_id': ['1738', '1523', '1892', '1456', '1789'],
        'name': ['Solar Farm Alpha', 'Wind Project Beta', 'Storage Unit Gamma', 'Hybrid Delta', 'Solar Epsilon'],
        'poi': ['Marcy', 'Marcy', 'Leeds', 'Athens', 'Dunwoodie'],
        'type': ['Solar', 'Wind', 'Storage', 'Hybrid', 'Solar'],
        'capacity_mw': [500, 300, 100, 250, 150],
        'developer': ['NextEra', 'Invenergy', 'Tesla', 'EDF', 'SunPower'],
        'score': [72, 68, 55, 81, 45],
        'recommendation': ['GO', 'CONDITIONAL', 'CONDITIONAL', 'GO', 'NO-GO']
    })

    print("Creating POI map...")
    html = create_poi_map(sample_data, color_by='recommendation')
    print(f"Map saved to: {MAP_DIR / 'poi_map.html'}")

    print("\nCreating congestion heatmap...")
    html = create_congestion_heatmap(sample_data)
    print(f"Heatmap saved to: {MAP_DIR / 'congestion_heatmap.html'}")
