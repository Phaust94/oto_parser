import math

__all__ = [
    "haversine",
    "dist_from_root",
]


# Warsaw center
ROOT_DICT = {
    "Warsaw": (52.23182630705096, 21.00591455254282),
    "Krakow": (50.06196857618123, 19.938187263875268),
}

# Radius of the Earth in kilometers
R = 6371.0


def haversine(lat1, lon1, lat2, lon2):
    # Convert latitude and longitude from degrees to radians
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    # Haversine formula
    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Distance in kilometers
    distance = R * c
    return distance


def dist_from_root(city: str, lat: float, lon: float):
    root = ROOT_DICT[city]
    return haversine(*root, lat, lon)
