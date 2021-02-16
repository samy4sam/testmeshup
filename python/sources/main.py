import json
import requests
from functools import reduce
from influxdb import InfluxDBClient
from classes import *
from datetime import datetime
from datetime import timezone
import re
import sys
import time
from pathlib import Path
import os
from dotenv import load_dotenv


def getEnvValue(key):
    pathToEnv = Path(str(Path(__file__).parents[3]) + '\.env')
    load_dotenv(pathToEnv.resolve())
    return os.getenv(key)


def readMapFile(path):
    with open(path) as json_file:
        data = json.load(json_file)
        return data


def getJsonObject(jsonString, path):
    try:
        for i in range(len(path)):
            if path[i].isdigit():
                path[i] = int(path[i])
        return reduce(lambda x, y: x[y], path, jsonString)
    except:
        return jsonString


def timestampToZulu(timestamp):
    return "{}Z".format(datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat(sep='T').replace('+00:00', ""))


def getRequest(url, headers={}, data={}):
    try:
        request = requests.request("GET", url, headers=headers, data=data)
        return request
    except:
        print("Fehler bei http-GET-Request. url: ", url)
        return None


def makeAllFLoats(jsonBody):
    for measurement in jsonBody:
        for tag in measurement['tags']:
            if isinstance(measurement['tags'][tag], str):
                try:
                    measurement['tags'][tag] = float(measurement['tags'][tag])
                except:
                    pass
            elif isinstance(measurement['tags'][tag], int):
                measurement['tags'][tag] = float(measurement['tags'][tag])
        for tag in measurement['fields']:
            if isinstance(measurement['fields'][tag], str):
                try:
                    measurement['fields'][tag] = float(measurement['fields'][tag])
                except:
                    pass
            elif isinstance(measurement['fields'][tag], int):
                measurement['fields'][tag] = float(measurement['fields'][tag])


def readFromInfluxDB(database, query):
    try:
        client = InfluxDBClient(host=database.host, port=database.port, username=database.username, password=database.password,
                                database=database.name)
        result = client.query(query)
        print("Daten aus DB gelesen")
        return result
    except:
        print("Fehler beim Lesen aus der InfluxDB: ", database.name)
        return None


def writeToInfluxDB(database, body):
    makeAllFLoats(body)
    try:
        client = InfluxDBClient(host=database.host, port=database.port, username=database.username, password=database.password,
                                database=database.name)
        if client.write_points(body, database=database.name):
            print("Daten in DB {} geschrieben.".format(database.name))
    except Exception as error:
        print("Fehler beim Schreiben in die InfluxDB: ", database.name, " Body: ", body)
        try:
            errorContent = json.loads(error.__dict__['content'])
            errorCode = error.__dict__['code']
            print("Code: ", errorCode)
            print("Error: ", errorContent['error'])
        except:
            print(error)


def stringPathToArrayWithPath(stringPath):
    try:
        path = stringPath.split("[].", 1)[1].split(".")
    except:
        path = stringPath.split(".")
    for i in range(len(path)):
        if path[i].isdigit():
            path[i] = int(path[i])
    return path


def getValueWithPath(stringPath, dataContainsValue):
    arrayPath = stringPathToArrayWithPath(stringPath)
    return reduce(lambda x, y: x[y], arrayPath, dataContainsValue)


def createInstanceOfDatapoint(datapoint):
    name = datapoint['name']
    pathToValue = datapoint['pathToValue']

    return Datapoint(name, pathToValue)


def createInstanceOfFilter(filter):
    try:
        pathToFilter = filter["pathToFilter"]
    except:
        pathToFilter = None
    filterValues = filter["filterValues"]
    return Filter(filterValues, pathToFilter)


def createInstanceOfElementDatasource(datasource, nameOfSource):
    token = datasource['elementToken']
    fields = []
    tags = []
    for field in datasource['fields']:
        fields.append(createInstanceOfDatapoint(field))
    for tag in datasource['tags']:
        tags.append(createInstanceOfDatapoint(tag))
    try:
        filter = createInstanceOfFilter(datasource['filter'])
    except:
        filter = None

    sourceTag = Datapoint('source', "")
    sourceTag.value = nameOfSource
    tags.append(sourceTag)
    return ElementDatasource(fields, tags, token, filter)


def createInstanceOfUrlDatasource(datasource, nameOfSource):
    url = datasource['url']
    fields = []
    tags = []
    for field in datasource['fields']:
        fields.append(createInstanceOfDatapoint(field))
    for tag in datasource['tags']:
        tags.append(createInstanceOfDatapoint(tag))
    try:
        filter = createInstanceOfFilter(datasource['filter'])
    except:
        filter = None

    sourceTag = Datapoint('source', "")
    sourceTag.value = nameOfSource
    tags.append(sourceTag)
    return UrlDatasource(fields, tags, url, filter)


def createInstances(map, host: Host):
    databaseList = []
    for database in map['databases']:
        measurementList = []
        nameDatabase = database['name']
        for measurement in database['measurements']:
            sourceList = []
            nameMeasurement = measurement['name']
            for source in measurement['sources']:
                datasourceList = []
                nameSource = source['name']
                protocol = source['protocol']
                for datasource in source['datasources']:
                    if protocol == 'url':
                        datasourceList.append(createInstanceOfUrlDatasource(datasource, nameSource))
                    elif protocol == 'elementIoT':
                        datasourceList.append(createInstanceOfElementDatasource(datasource, nameSource))
                sourceList.append(Source(nameSource, protocol, datasourceList))
            measurementList.append(Measurement(nameMeasurement, sourceList))
        databaseList.append(Database(nameDatabase, measurementList, host=host))
    return databaseList


def splitFilterPath(path):
    if '$' in path:
        return [path.split("$.", 1)[0].split("."), path.split("$.", 1)[1].split(".")]
    else:
        return None


def splitLoopPath(path):
    if '%' in path:
        return [path.split("%.", 1)[0].split("."), path.split("%.", 1)[1].split(".")]
    else:
        return None


def timeconvert(timeStr: str):
    if timeStr.endswith('.000+01:00'):
        timeStr = timeStr[:-10]
        timeStr = timeStr + 'Z'
        return timeStr


def setValueToDatapoint(datapoint, value, measurementForInflux=None):
    if datapoint.name == 'time' and measurementForInflux is not None:
        if isinstance(value, int):
            measurementForInflux.time = timestampToZulu(value)
            return None
        else:
            if value.endswith('+01:00'):
                measurementForInflux.time = timeconvert(value)
            else:
                measurementForInflux.time = value
            return None
    elif 'time' in datapoint.name:
        if isinstance(datapoint.value, int):
            datapoint.value = timestampToZulu(value)
    else:
        datapoint.value = value


def checkTypeOfCoordinate(packet):
    lv95Pattern = re.compile("(\d{4,}[.]\d{1,2})")
    longitudeTag = None
    latitudeTag = None
    for datapoint in packet.tags:
        if any(x in datapoint.name for x in ["long"]):
            try:
                if re.match(lv95Pattern, datapoint.value):
                    longitudeTag = datapoint
            except:
                try:
                    if re.match(lv95Pattern, str(datapoint.value)):
                        longitudeTag = datapoint
                except:
                    print("Fehler bei Abgleich der Koordinaten mit Regex")
        if any(x in datapoint.name for x in ["lat"]):
            try:
                if re.match(lv95Pattern, datapoint.value):
                    latitudeTag = datapoint
            except:
                try:
                    if re.match(lv95Pattern, str(datapoint.value)):
                        latitudeTag = datapoint
                except:
                    print("Fehler bei Abgleich der Koordinaten mit Regex")
        if datapoint.name != 'time' and 'time' in datapoint.name:
            if isinstance(datapoint.value, int):
                datapoint.value = timestampToZulu(datapoint.value)

    if longitudeTag is not None and latitudeTag is not None:
        url = "http://geodesy.geo.admin.ch/reframe/lv95towgs84?easting={}&northing={}".format(longitudeTag.value,
                                                                                              latitudeTag.value)
        response = getRequest(url).json()
        longitudeTag.value = response['coordinates'][0]
        latitudeTag.value = response['coordinates'][1]

    longitudeTag = None
    latitudeTag = None

    for datapoint in packet.fields:
        if any(x in datapoint.name for x in ["long"]):
            try:
                if re.match(lv95Pattern, datapoint.value):
                    longitudeTag = datapoint
            except:
                try:
                    if re.match(lv95Pattern, str(datapoint.value)):
                        longitudeTag = datapoint
                except:
                    print("Fehler bei Abgleich der Koordinaten mit Regex")
        if any(x in datapoint.name for x in ["lat"]):
            try:
                if re.match(lv95Pattern, datapoint.value):
                    latitudeTag = datapoint
            except:
                try:
                    if re.match(lv95Pattern, str(datapoint.value)):
                        latitudeTag = datapoint
                except:
                    print("Fehler bei Abgleich der Koordinaten mit Regex")
        if datapoint.name != 'time' and 'time' in datapoint.name:
            if isinstance(datapoint.value, int):
                datapoint.value = timestampToZulu(datapoint.value)

    if longitudeTag is not None and latitudeTag is not None:
        url = "http://geodesy.geo.admin.ch/reframe/lv95towgs84?easting={}&northing={}".format(longitudeTag.value,
                                                                                              latitudeTag.value)
        response = getRequest(url).json()
        longitudeTag.value = response['coordinates'][0]
        latitudeTag.value = response['coordinates'][1]


def getZoneFromPosition(zonen, pos_latitude, pos_longitude):
    url = "https://swisspost.opendatasoft.com/api/records/1.0/search/?dataset=plz_verzeichnis_v2&geofilter.distance={},{}".format(pos_latitude,
                                                                                                                                  pos_longitude)

    response = getRequest(url).json()
    commune = response['records'][0]['fields']['ortbez18']

    for zone in zonen:
        for gemeinde in zonen[zone]:
            if gemeinde == commune:
                return zone


def createJsonBody(packetsForMeasurement, nameMeasurement):
    jsonBody = []
    try:
        for packet in packetsForMeasurement:
            json_body = {'measurement': nameMeasurement}
            field_body = {}
            tag_body = {}
            checkTypeOfCoordinate(packet)
            for datapoint in packet.fields:
                json_body['time'] = packet.time
                if datapoint.name != 'time':
                    field_body[datapoint.name] = datapoint.value
            for datapoint in packet.tags:
                tag_body[datapoint.name] = datapoint.value
            if 'zone' in tag_body:
                zonen = readMapFile('zone_mapping.json')
                tag_body['zone'] = getZoneFromPosition(zonen=zonen, pos_longitude=tag_body['pos_longitude'],
                                                       pos_latitude=tag_body['pos_latitude'])

            json_body['tags'] = tag_body
            json_body['fields'] = field_body
            jsonBody.append(json_body)
    except:
        # Keine Daten vorhanden. Leeres JSONBody zurück geben
        return []
    return jsonBody


def getDataFromUrl(database, measurement, urlDatasource, source):
    fieldNeedFilter = False
    loopNeeded = False
    timeNeedLoop = False
    filter = urlDatasource.filter
    fields = urlDatasource.fields
    tags = urlDatasource.tags
    url = urlDatasource.url
    tagList = []
    dataFromRequest = getRequest(url).json()
    tagNeedFilter = False

    packets = []
    if filter is not None:
        fieldNeedFilter = True

    for field in fields:
        if '%' in field.pathToValue:
            if field.name == "time":
                timeNeedLoop = True
            loopNeeded = True
            field.needsLoop = True

    for tag in tags:
        if fieldNeedFilter:
            if '$' in tag.pathToValue:
                tagNeedFilter = True
        if '%' in tag.pathToValue:
            print("Tags mit Loop nicht erlaubt")
            return
    if fieldNeedFilter and tagNeedFilter:
        splittedFilterPath = splitFilterPath(filter.pathToFilter)
        filterValues = filter.filterValues
        features = getJsonObject(dataFromRequest, splittedFilterPath[0])
        for feature in getJsonObject(dataFromRequest, splittedFilterPath[0]):
            valueToFilter = getJsonObject(feature, splittedFilterPath[1])
            if valueToFilter in filterValues:
                tagList = []
                packetForInflux = PacketForInflux(tagList, fields)
                for tag in tags:
                    if tag.value is None:
                        if tag.pathToValue != "":
                            splittedValuePath = splitFilterPath(tag.pathToValue)
                            value = getJsonObject(feature, splittedValuePath[1])
                            tagWithValue = Datapoint(tag.name, value=value)

                            tagList.append(tagWithValue)
                for field in fields:
                    splittedValuePath = splitFilterPath(field.pathToValue)
                    value = getJsonObject(feature, splittedValuePath[1])
                    setValueToDatapoint(field, value, packetForInflux)
                sourceTag = Datapoint("source", value=source)
                tagList.append(sourceTag)
                zoneTag = Datapoint("zone", value="")
                tagList.append(zoneTag)
                packets.append(packetForInflux)
        requestBody = createJsonBody(packets, measurement.name)
        writeToInfluxDB(database, requestBody)
        return
    elif tagNeedFilter:
        splittedFilterPath = splitFilterPath(filter.pathToFilter)
        filterValues = filter.filterValues
        for feature in getJsonObject(dataFromRequest, splittedFilterPath[0]):
            valueToFilter = getJsonObject(feature, splittedFilterPath[1])
            if valueToFilter in filterValues:
                for tag in tags:
                    if tag.value is None:
                        if tag.pathToValue != "":
                            splittedValuePath = splitFilterPath(tag.pathToValue)
                            value = getJsonObject(feature, splittedValuePath[1])
                            tagWithValue = Datapoint(tag.name, value=value)
                            tagList.append(tagWithValue)
    else:
        for tag in tags:
            if tag.value is None:
                if tag.pathToValue != "":
                    value = getJsonObject(dataFromRequest, tag.pathToValue.split('.'))
                    tagWithValue = Datapoint(tag.name, value=value)
                    tagList.append(tagWithValue)
    sourceTag = Datapoint("source", value=source)
    tagList.append(sourceTag)
    zoneTag = Datapoint("zone", value="")
    tagList.append(zoneTag)
    if not loopNeeded:
        if fieldNeedFilter:
            splittedFilterPath = splitFilterPath(filter.pathToFilter)
            filterValues = filter.filterValues
            packetForInflux = PacketForInflux(tagList, fields)
            for feature in getJsonObject(dataFromRequest, splittedFilterPath[0]):
                valueToFilter = getJsonObject(feature, splittedFilterPath[1])
                if valueToFilter in filterValues:
                    for field in fields:
                        splittedValuePath = splitFilterPath(field.pathToValue)
                        value = getJsonObject(feature, splittedValuePath[1])
                        setValueToDatapoint(field, value, packetForInflux)
                packets.append(packetForInflux)
        else:
            packetForInflux = PacketForInflux(tagList, fields)
            for field in fields:
                value = getJsonObject(dataFromRequest, field.pathToValue.split('.'))
                setValueToDatapoint(field, value, packetForInflux)
            packets.append(packetForInflux)
    else:
        if not fieldNeedFilter:
            if timeNeedLoop:
                loopPath = splitLoopPath(fields[0].pathToValue)[0]
                for feature in getJsonObject(dataFromRequest, loopPath):
                    packetForInflux = PacketForInflux(tagList, fields)
                    for field in fields:
                        if field.needsLoop:
                            splittedValuePath = splitLoopPath(field.pathToValue)
                            value = getJsonObject(feature, splittedValuePath[1])
                        else:
                            value = getJsonObject(feature, field.pathToValue)
                        setValueToDatapoint(field, value, packetForInflux)
                    packets.append(packetForInflux)
        else:
            print("Muss implementiert werden")
            # TODO: implementieren Loop mit Filter

    requestBody = createJsonBody(packets, measurement.name)
    writeToInfluxDB(database, requestBody)

def getelementDeviceInfos(url):
    response = getRequest(url).json()['body']
    print(response)
    return response

def elementRequest(database, measurement, deviceID, deviceName, lastReading, fields, token):
    elementIoTEndPoint = getEnvValue('ZENNER_ENDPOINT')
    request = "/api/v1/devices/{}".format(deviceID)
    responseBody = []
    json_body = []
    deviceInfo = getelementDeviceInfos("{}{}?auth={}".format(elementIoTEndPoint, request, token))
    while True:
        url = "{}{}/readings?limits=200&sort=measured_at&sort_direction=asc&auth={}&after={}".format(
            elementIoTEndPoint, request, token, lastReading)
        try:
            response = getRequest(url).json()
            while 'error' in response:
                print("To many Requests. Send request again")
                time.sleep(1)
                response = getRequest(url).json()
            response = response['body']
            if len(response) == 0:
                return json_body
            if lastReading == response[len(response) - 1]['measured_at']:
                return json_body
            lastReading = response[len(response) - 1]['measured_at']
            responseBody.append(response)
            for data in response:
                try:
                    measured_at = data['measured_at']
                    dataPacket = data['data']
                    #if dataPacket.hasOwnProperty('unparseable'):
                    #    #Datenpacket enthählt Daten die nicht gelesen werden konnten. Packet soll übersprungen werden.
                    #    continue
                    location = deviceInfo['location']['coordinates']
                    try:
                        objekt = deviceInfo['profile_data'][0]['data']['objekt']
                    except:
                        objekt = None
                    body = createInfluxRequestBodyElement(database.name, measurement.name, deviceID, deviceName, location, dataPacket, fields, measured_at, objekt)
                    json_body.append(body)
                except:
                    print("Datenpacket von Elements enthielt unlesbare Daten")
        except:
            return json_body


def createInfluxRequestBodyElement(database, measurement, deviceId, deviceName, location, dataPacket, fields,
                                   measured_at, object):
    json_body = {"measurement": measurement}
    tags_body = {}
    values_body = {}
    for field in fields:

        if field.pathToValue in dataPacket.keys():
            values_body[field.name] = dataPacket[field.pathToValue]
    tags_body['device_eui'] = deviceId
    tags_body['device_name'] = deviceName
    tags_body['pos_longitude'] = location[0]
    tags_body['pos_latitude'] = location[1]
    tags_body['object'] = object
    json_body['time'] = measured_at
    json_body['tags'] = tags_body
    json_body['fields'] = values_body
    return json_body


def getLastWritingInInfluxDB(database, measurement, deviceName):

    query = "SELECT * AS \"battery_voltage\" FROM \"{}\" WHERE \"device_name\"='{}'ORDER BY DESC LIMIT 1".format(
        measurement.name, deviceName)

    try:
        result = readFromInfluxDB(database, query)
        points = list(result.get_points())
        return "2021-01-01T10:00:00.000000Z"
        #return points[0]['time']
    except:
        return "2020-01-01T10:00:00.000000Z"
        # return"2020-12-13T14:32:19.471463Z"


def getDeviceListFromElement(token):
    elementIoTEndPoint = "https://element-iot.ch"
    request = "/api/v1/devices/"
    url = "{}{}?limit=200&auth={}".format(elementIoTEndPoint, request, token)

    print(url)
    deviceList = {}
    request = getRequest(url)

    try:
        for device in request.json()['body']:
            deviceList[device['name']] = device['interfaces'][0]['device_id']
    except:
        if request.content == b'{"error":"Too Many Requests"}':
            return getDeviceListFromElement(token)
        else:
            print("Error: Keine lesbaren Daten von Request Response. URL:")
            print(url)
    return deviceList


def getDataFromElement(database, measurement, elementDatasource):
    filters = elementDatasource.filter.filterValues
    fields = elementDatasource.fields
    token = getEnvValue('ZENNER_TOKEN')
    listWithDeviceIds = getDeviceListFromElement(token)
    for device in filters:
        lastReading = getLastWritingInInfluxDB(database, measurement, device)
        json_body = elementRequest(database, measurement, listWithDeviceIds[device], device, lastReading, fields, token)
        writeToInfluxDB(database, json_body)


def getData(databasesInstance, databaseName):
    for database in databasesInstance:
        if database.name == databaseName:
            for measurement in database.measurements:
                for source in measurement.sources:
                    for datasource in source.datasources:
                        if source.protocol == 'url':
                            getDataFromUrl(database, measurement, datasource, source.name)
                        elif source.protocol == 'elementIoT':
                            getDataFromElement(database, measurement, datasource)


if __name__ == '__main__':
    influx_endpoint = getEnvValue('INFLUX_ENDPOINT').split(':')

    mapFile = readMapFile('SourceMapping.json')
    if len(sys.argv) != 2:
        try:
            getDataFor = 'swaLauraTest'
            influxDBHost = Host(hostAddress="localhost", hostPort=int(influx_endpoint[2]), hostUsername=getEnvValue('INFLUXDB_ADMIN_USER'),
                                hostPassword=getEnvValue('INFLUXDB_ADMIN_PASSWORD'))
            databases = createInstances(mapFile, host=influxDBHost)
            getData(databases, databaseName=getDataFor)
        except Exception:
            print('Invalid Numbers of Arguments. Script will be terminated.')

    else:
        try:
            getDataFor = str(sys.argv[1])

            # Host-Setting #
            influxDBHost = Host(hostAddress=influx_endpoint[0]+':'+influx_endpoint[1], hostPort=int(influx_endpoint[2]), hostUsername=getEnvValue(
                'INFLUXDB_ADMIN_USER'),
                                hostPassword=getEnvValue('INFLUXDB_ADMIN_PASSWORD'))
            databases = createInstances(mapFile, host=influxDBHost)
            getData(databases, databaseName=getDataFor)
        except ValueError:
            print("Input-Argument muss Name einer Database sein!")
