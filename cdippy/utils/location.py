from math import radians, sin, cos, sqrt, asin, atan2, degrees


class Location:
    """A class to work with latitude/longitude locations."""

    R = 6372.8  # Earth radius in kilometers
    kmToNm = 0.539957  # 1KM ==  .539957 NM

    def __init__(self, latitude: float, longitude: float):

        self.latitude = latitude
        self.longitude = longitude

    def write_lat(self):
        return repr(self.latitude)

    def write_lon(self):
        return repr(self.longitude)

    def write_loc(self):
        return self.write_lat() + " " + self.write_lon()

    def decimal_min_loc(self):
        longitude = self.longitude
        if longitude < 0:
            longitude = longitude * -1
        dmLon = divmod(longitude, 1)
        minLon = dmLon[1]
        minLon = minLon * 60
        minLon = format(minLon, "2.3f")
        latitude = self.latitude
        if latitude < 0:
            latitude = latitude * -1
        dmLat = divmod(latitude, 1)
        minLat = dmLat[1]
        minLat = minLat * 60
        minLat = format(minLat, "2.3f")

        if self.longitude < 0:
            dLon = dmLon[0] * -1
            dLon = int(dLon)
        else:
            dLon = dmLon[0]
            dLon = int(dLon)

        if self.latitude < 0:
            dLat = dmLat[0] * -1
            dLat = int(dLat)
        else:
            dLat = dmLat[0]
            dLat = int(dLat)

        return {"dlon": dLon, "mlon": minLon, "dlat": dLat, "mlat": minLat}

    def get_distance(self, loc):
        """Return the distance between 2 locations in nautical miles."""
        rdn_longitude, rdn_latitude, loc.longitude, loc.latitude = map(
            radians, [self.longitude, self.latitude, loc.longitude, loc.latitude]
        )

        dLat = loc.latitude - rdn_latitude
        dLon = loc.longitude - rdn_longitude

        a = (
            sin(dLat / 2) ** 2
            + cos(rdn_latitude) * cos(loc.latitude) * sin(dLon / 2) ** 2
        )
        c = 2 * asin(sqrt(a))
        return self.R * c * self.kmToNm

    def get_distance_formatted(self, loc):
        """Return the distance between 2 locations in nautical miles formatted
        to two decimal places.
        """
        return format(self.get_distance(loc), ".2f")

    def get_direction(self, loc):
        """Return the direction to a supplied location

        The formulae used is the following:
        θ = atan2(sin(Δlong).cos(lat2),
              cos(lat1).sin(lat2) − sin(lat1).cos(lat2).cos(Δlong))
        """
        rdn_longitude, rdn_latitude, loc.longitude, loc.latitude = map(
            radians, [self.longitude, self.latitude, loc.longitude, loc.latitude]
        )

        dLon = loc.longitude - rdn_longitude
        x = sin(dLon) * cos(loc.latitude)
        y = cos(rdn_latitude) * sin(loc.latitude) - sin(rdn_latitude) * cos(
            loc.latitude
        ) * cos(dLon)
        direction = atan2(x, y)
        direction = degrees(direction)
        direction = (direction + 360) % 360
        return direction
