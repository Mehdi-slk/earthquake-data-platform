import requests


def extract_earthquakes(starttime, endtime):
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"

    params = {
        "format": "geojson",
        "starttime": starttime,
        "endtime": endtime
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    earthquakes = []

    for feature in data["features"]:
        properties = feature["properties"]
        coordinates = feature["geometry"]["coordinates"]

        earthquake = {
            "event_id": feature["id"],
            "magnitude": properties["mag"],
            "place": properties["place"],
            "event_time": properties["time"],
            "updated_time": properties["updated"],
            "tz": properties["tz"],
            "felt": properties["felt"],
            "cdi": properties["cdi"],
            "mmi": properties["mmi"],
            "alert": properties["alert"],
            "status": properties["status"],
            "tsunami": properties["tsunami"],
            "significance": properties["sig"],
            "network": properties["net"],
            "code": properties["code"],
            "ids": properties["ids"],
            "sources": properties["sources"],
            "types": properties["types"],
            "nst": properties["nst"],
            "dmin": properties["dmin"],
            "rms": properties["rms"],
            "gap": properties["gap"],
            "magnitude_type": properties["magType"],
            "event_type": properties["type"],
            "title": properties["title"],
            "longitude": coordinates[0],
            "latitude": coordinates[1],
            "depth": coordinates[2]
        }

        earthquakes.append(earthquake)

    return earthquakes


earthquakes = extract_earthquakes(
    starttime="2026-09-01",
    endtime="2026-09-02"
)
print(f"Number of earthquakes: {len(earthquakes)}")
print(earthquakes[0])
# print(f"Number of earthquakes: {len(earthquakes)}")

# for earthquake in earthquakes:
#     print(earthquake)