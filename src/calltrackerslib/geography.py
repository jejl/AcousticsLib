"""Geographic utilities for the CallTrackers project.

Ported from CallTrackersAdmin/my_app/calltrackers/geography.py.

The shapefile path is no longer read from a Config singleton at import time;
it is passed explicitly to :func:`load_naturetrackers_squares` so the module
is importable without environment variables being set.

Default Tasmanian coordinates used when no GPS fix is available::

    TAS_CENTRE_LAT = -42.0017
    TAS_CENTRE_LON = 146.6101
"""
import os
from math import asin, cos, radians, sin, sqrt
from pathlib import Path
from typing import Optional

from loguru import logger

TAS_CENTRE_LAT = -42.0017
TAS_CENTRE_LON = 146.6101

# Default shapefile location (overridable via env var or explicit argument)
_DEFAULT_SQUARES_PATH = os.getenv(
    "CALLTRACKERS_SQUARES_FILEPATH",
    "data/Tas_Grid_GDA_1994_MGA_Zone_55_v2.shp",
)


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the great-circle distance in metres between two lat/lon points."""
    R = 6_371_000
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return R * 2 * asin(sqrt(a))


def group_by_location(
    files: list[dict],
    max_dist: float = 100,
) -> list[list[int]]:
    """Group file records by proximity.

    Args:
        files:    List of dicts, each with optional ``lat`` and ``lon`` keys.
        max_dist: Maximum distance in metres to consider two records co-located.

    Returns:
        List of groups; each group is a list of indices into *files*.
        Files without lat/lon are placed in singleton groups.
    """
    groups: list[list[int]] = []
    used: set[int] = set()

    for i, f in enumerate(files):
        if i in used or f.get("lat") is None or f.get("lon") is None:
            continue
        group = [i]
        for j, f2 in enumerate(files):
            if (
                j != i
                and j not in used
                and f2.get("lat") is not None
                and f2.get("lon") is not None
                and haversine(f["lat"], f["lon"], f2["lat"], f2["lon"]) < max_dist
            ):
                group.append(j)
        used.update(group)
        groups.append(group)

    # Append files with no location as singletons
    for i, f in enumerate(files):
        if i not in used:
            groups.append([i])

    return groups


def get_square_number(lat: float, lon: float, gdf_squares_latlon) -> int:
    """Return the NatureTrackers grid square GID containing the point, or 0.

    Args:
        lat:                Latitude (decimal degrees).
        lon:                Longitude (decimal degrees).
        gdf_squares_latlon: GeoDataFrame in EPSG:4326 as returned by
                            :func:`load_naturetrackers_squares`.
    """
    from shapely.geometry import Point
    pt = Point(lon, lat)
    for _, row in gdf_squares_latlon.iterrows():
        if row.geometry.contains(pt):
            return row.GID
    return 0


def load_naturetrackers_squares(
    squares_filepath: Optional[str | Path] = None,
):
    """Load and reproject the NatureTrackers grid square shapefile.

    Args:
        squares_filepath: Path to the ``.shp`` file.  Falls back to the
                          ``CALLTRACKERS_SQUARES_FILEPATH`` environment variable,
                          then to ``data/Tas_Grid_GDA_1994_MGA_Zone_55_v2.shp``.

    Returns:
        GeoDataFrame in EPSG:4326 (WGS84 lat/lon).

    Raises:
        FileNotFoundError: If the shapefile cannot be found.
    """
    import geopandas as gpd

    path = Path(squares_filepath or _DEFAULT_SQUARES_PATH)
    if not path.exists():
        raise FileNotFoundError(f"Squares shapefile not found: {path}")

    logger.info(f"Loading NatureTrackers squares shapefile: {path}")
    gdf = gpd.read_file(path)
    gdf = gdf.set_crs("EPSG:28355", allow_override=True)
    return gdf.to_crs("EPSG:4326")
