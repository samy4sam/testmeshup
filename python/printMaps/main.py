import json
from classes import *
import folium
from influxdb import InfluxDBClient
import pandas as pd
import requests


def readMapFile(path):
    with open(path, encoding='utf-8') as json_file:
        data = json.load(json_file)
        return data


def getRequest(url, headers={}, data={}):
    try:
        request = requests.request("GET", url, headers=headers, data=data)
        return request
    except:
        print("Fehler bei http-GET-Request. url: ", url)
        return None


def readFromInfluxDB(database, query):
    try:
        client = InfluxDBClient(host=database.host, port=database.port, username=database.username, password=database.password,
                                database=database.name)
        result = client.query(query)
        print("Daten aus DB gelesen")
        return result
    except  Exception as error:
        print("Fehler beim Lesen aus der InfluxDB: ", database.name)
        print(error)
        return None


# Only for development
def reduceShiftInGeoJsonData():
    state_geo = json.load(open('langrickenbach.json'))

    communePoints = []
    for commune in state_geo['geometry']['coordinates']:
        points = []
        for point in commune:
            long = [point][0][0] - 0.0012
            lat = [point][0][1] - 0.0014
            coords = [long, lat]
            points.append(coords)
        communePoints.append(points)

    print(communePoints)


def addGeoJsonToMap(mapReference, file, color):
    state_geo = json.load(open(file))
    folium.Choropleth(geo_data=state_geo,
                      fill_color=color,

                      ).add_to(mapReference)


def addPositionsToMap(locations, color, mapReference, name, dfWithNames):
    tooltip = 'Click me!'
    for point in range(0, len(locations)):
        try:
            source = dfWithNames.loc[dfWithNames.pos_latitude == locations[point][0]].name.item()
        except:
            source = ''
            print('Error')
            pass
        folium.Marker(location=locations[point],
                      icon=folium.Icon(color=color),
                      popup='<h3>' + name + '</h3><p>longitude: ' + locations[point][1] + '</p><p>latitude: ' + locations[point][0] +
                            '</p><p>source: ' + source + '</p>',
                      # tooltip=tooltip
                      ).add_to(mapReference)


def printOverviewmap(soilDataDF, environmentDataDF, weatherDataDF, weatherPredictionDataDF):
    locationsSoilSensorsList = soilDataDF[['pos_latitude', 'pos_longitude']].drop_duplicates().values.tolist()
    locationsSWABoxesList = environmentDataDF[['pos_latitude', 'pos_longitude']].drop_duplicates().values.tolist()
    locationsWeatherStationsList = weatherDataDF[['pos_latitude', 'pos_longitude']].drop_duplicates().values.tolist()
    locationsWeatherPredictionStationsList = weatherPredictionDataDF[['pos_latitude', 'pos_longitude']].drop_duplicates().values.tolist()

    swaMap = folium.Map(location=[47.5712, 9.2941], zoom_start=12)

    addPositionsToMap(locations=locationsSoilSensorsList, color="red", mapReference=swaMap, name="Soil-Sensor", dfWithNames=soilDataDF[[
        'pos_latitude', 'pos_longitude', 'name']].drop_duplicates())
    addPositionsToMap(locations=locationsSWABoxesList, color="green", mapReference=swaMap, name="SWA-Box", dfWithNames=environmentDataDF[[
        'pos_latitude', 'pos_longitude', 'name']].drop_duplicates())
    addPositionsToMap(locations=locationsWeatherStationsList, color="purple", mapReference=swaMap, name="Weather-Station", dfWithNames=weatherDataDF[[
        'pos_latitude', 'pos_longitude', 'source']].drop_duplicates().rename(columns={'source': 'name'}))
    addPositionsToMap(locations=locationsWeatherPredictionStationsList, color="black", mapReference=swaMap, name="Weather-Prediction-Station",
                      dfWithNames=weatherPredictionDataDF[['pos_latitude', 'pos_longitude', 'source']].drop_duplicates().rename(columns={'source':
                                                                                                                                             'name'}))

    addGeoJsonToMap(swaMap, "amriswil.json", "lightblue")
    addGeoJsonToMap(swaMap, "altnau.json", "blue")
    addGeoJsonToMap(swaMap, "guettingen.json", "green")
    addGeoJsonToMap(swaMap, "langrickenbach.json", "grey")

    swaMap.add_child(folium.ClickForMarker(popup='Waypoint'))
    swaMap.add_child(folium.LatLngPopup())

    swaMap.save("swaMap.html")


def getZoneFromPosition(pos_latitude, pos_longitude):
    url = "https://swisspost.opendatasoft.com/api/records/1.0/search/?dataset=plz_verzeichnis_v2&geofilter.distance={},{}".format(pos_latitude,
                                                                                                                                  pos_longitude)

    response = getRequest(url).json()
    commune = response['records'][0]['fields']['ortbez18']
    print(url)


if __name__ == '__main__':
    influxDBHost = Host(hostAddress="localhost", hostPort=8086, hostUsername="admin", hostPassword="rr#ReZmL&IM5#O68%sW7VVWaT")

    swaBrunoDatabase = InfluxDatabase(name="swaBruno", host=influxDBHost)
    swaWeatherStorageDatabase = InfluxDatabase(name="swaWeatherStorageTest", host=influxDBHost)
    swaLauraDatabase = InfluxDatabase(name="swaLaura", host=influxDBHost)

    soilDataDF = pd.DataFrame.from_dict(readFromInfluxDB(swaLauraDatabase, "SELECT * from soil").get_points())
    environmentDataDF = pd.DataFrame.from_dict(readFromInfluxDB(swaLauraDatabase, "SELECT * from environment").get_points())
    weatherDataDF = pd.DataFrame.from_dict(readFromInfluxDB(swaWeatherStorageDatabase, "SELECT * from weather").get_points())
    weatherPredictionDataDF = pd.DataFrame.from_dict(readFromInfluxDB(swaWeatherStorageDatabase, "SELECT * from weatherPrediction").get_points())

    printOverviewmap(soilDataDF, environmentDataDF, weatherDataDF, weatherPredictionDataDF)

    getZoneFromPosition(pos_latitude=47.5689, pos_longitude=9.2833)
