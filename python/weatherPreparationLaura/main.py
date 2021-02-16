from classes import *
import requests
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import json
import time
from pathlib import Path
import os
from dotenv import load_dotenv
from datetime import datetime
import scipy.interpolate
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as pltDates
import matplotlib.colors as pltColors
import pandas as pd

influxDBHost = None
swaLauraDatabase = None
swaWeatherStorageDatabase = None
objects = []


def getEnvValue(key):
    try:
        load_dotenv()
        return os.getenv(key)
    except:
        try:
            pathToEnv = Path(str(Path(__file__).parents[2]) + '\.env')
            load_dotenv(pathToEnv.resolve())
            return os.getenv(key)
        except:
            print("Error! Env-File not found.")


def readMapFile(path):
    with open(path, encoding='utf-8') as json_file:
        data = json.load(json_file)
        return data


def addPoint(scat, new_point, c='k'):
    old_off = scat.get_offsets()
    new_off = np.concatenate([old_off, np.array(new_point, ndmin=2)])
    old_c = scat.get_facecolors()
    new_c = np.concatenate([old_c, np.array(pltColors.to_rgba(c), ndmin=2)])

    scat.set_offsets(new_off)
    scat.set_facecolors(new_c)

    scat.axes.figure.canvas.draw_idle()


def printInterpolationMap(X, Y, Z, index, ax, firstTime, listWithNan, positionTable, minValue, maxValue, nameValue):
    plt.pcolormesh(X, Y, Z, shading='auto', vmin=minValue, vmax=maxValue)
    ax.set_title(index)
    try:
        xPos = []
        yPos = []
        for i in range(0, len(listWithNan)):
            value = listWithNan[i]
            if pd.notna(value):
                xPos.append(float(positionTable.iloc[i].pos_longitude))
                yPos.append(float(positionTable.iloc[i].pos_latitude))
        plotStation, = plt.plot(xPos, yPos, "or", label="Wetterstation")
    except:
        pass

    if firstTime:
        plt.legend()
        cbar = plt.colorbar()
        cbar.ax.get_yaxis().labelpad = 15
        cbar.ax.set_ylabel(nameValue, rotation=270)
        plt.axis("equal")
        plt.annotate(text='Amriswil', xy=[9.2943, 47.5455], xytext=[9.30, 47.475], arrowprops=dict(arrowstyle="->"))
        plt.annotate(text='Güttingen', xy=[9.2869, 47.6034], xytext=[9.35, 47.65], arrowprops=dict(arrowstyle="->"))
        plt.annotate(text='Altnau', xy=[9.2592, 47.6106], xytext=[9.25, 47.65], arrowprops=dict(arrowstyle="->"))

    plt.draw()
    plt.pause(1e-17)
    time.sleep(0.1)
    try:
        plotStation.remove()
    except:
        pass


def getValueAtPointFromInterpolation(interpolatedData, point):
    searchPointLatitude = point[0]
    searchPointLongitude = point[1]

    # Get nearest coordinates in the Interpolated-Data-DataFrame
    xSearch = interpolatedData.index.get_loc(searchPointLatitude, method='nearest')
    ySearch = interpolatedData.columns.get_loc(searchPointLongitude, method='nearest')

    # Get the Value at the asked position
    return interpolatedData.iat[xSearch, ySearch]


def interpolateOverTime(valuesTable, positionTable, tableWithNan, points, objectNames, countPositions, nameValue, plotMap):
    maxValue = valuesTable.max().max()
    minValue = valuesTable.min().max()
    firstTime = True
    if plotMap:
        fig, ax = plt.subplots()
    i = 0
    returnDataFrame = pd.DataFrame()
    for index, row in valuesTable.iterrows():
        i = i + 1
        try:
            listWithNan = list(tableWithNan.iloc[tableWithNan.index.get_loc(index)].reindex(range(countPositions), fill_value=np.NaN))
        except:
            pass
        x = list(map(float, list(positionTable.pos_longitude)))
        y = list(map(float, list(positionTable.pos_latitude)))
        z = list(row.reindex(range(countPositions), fill_value=np.NaN).fillna(method='ffill').fillna(method="backfill"))

        X = np.linspace(9.12974184776004, 9.4505631084101)
        Y = np.linspace(47.4957231059287, 47.624878998826)
        X, Y = np.meshgrid(X, Y)  # 2D grid for interpolation

        interp = scipy.interpolate.LinearNDInterpolator(list(zip(x, y)), z, rescale=False)
        Z = interp(X, Y)

        # Save interpolated data in DataFrame
        xCoordinates = X[0]
        yCoordinates = []
        for coordinatePacket in Y:
            yCoordinates.append(coordinatePacket[0])
        interpolatedData = pd.DataFrame(data=Z, index=yCoordinates, columns=xCoordinates)

        listWithPoints = [index]
        listWithColumnNames = ['time']
        i = 0

        for point in points:
            valueAtPoint = getValueAtPointFromInterpolation(interpolatedData, point)
            # Save Value in list
            listWithPoints.append(valueAtPoint)
            listWithColumnNames.append(objectNames[i])
            i = i + 1
        #
        dfTemp = pd.DataFrame([listWithPoints], columns=listWithColumnNames)

        returnDataFrame = returnDataFrame.append(dfTemp, ignore_index=True)

        if plotMap:
            printInterpolationMap(X, Y, Z, index, ax, firstTime, listWithNan, positionTable, minValue, maxValue, nameValue)
        firstTime = False

    return returnDataFrame.set_index('time')


def plotValuesOverTime(interpolatedPoints, valueName, pointNames):
    index = 0
    for (columnName, columnData) in interpolatedPoints.iteritems():
        listWithValues = list(columnData.values)
        listWithDateTimes = list(columnData.index)
        fig, ax = plt.subplots()
        dates = pltDates.date2num(listWithDateTimes)
        ax.plot_date(dates, listWithValues, '-')
        xAxisFormat = pltDates.DateFormatter('%y-%m-%d %H:%M')
        ax.xaxis.set_major_formatter(xAxisFormat)
        ax.set_title(valueName + '-Verlauf in ' + pointNames[index])
        fig.autofmt_xdate()
        plt.show()
        index = index + 1


def interpolate(df, points: [int], parameterName: str, objectNames: [str], plotMap: bool, lastWritingDateTime):
    nameValue = parameterName

    # Ignore Weather-Station in Jona
    df = df[df.pos_latitude != '47.22391075015165'].reset_index()

    # Create Table with Positions (latitude, longitude)
    positionTable = pd.DataFrame({'pos_latitude': df['pos_latitude'].astype('category'), 'pos_longitude': df['pos_longitude']})

    # Use the Index of the Position-Table as the foreign Keys for the Values-Tables
    foreignKeys = positionTable['pos_latitude'].cat.codes
    positionTable = positionTable.set_index(foreignKeys).drop_duplicates().sort_index()
    try:
        valuesTable = pd.DataFrame({'time': df.time, nameValue: df[nameValue], 'position': foreignKeys}).set_index('time')
    except KeyError as err:
        print("Error: Parameter {} is not present in the source database -> will be ignored.".format(err))
        return None

    # prepare value-Table
    valuesTable = valuesTable.pivot_table(index='time', columns='position', values=nameValue)

    # tableWithNan is needed to plot later active measuring stations
    tableWithNan = valuesTable[lastWritingDateTime::]

    # Get the number of measuring stations
    countPositions = positionTable.shape[0]

    # Always same shape
    valuesTable = valuesTable.reindex(columns=range(countPositions))

    # Fill values if there are missing values
    valuesTable = valuesTable.fillna(method='ffill').fillna(method='backfill')

    # If there are columns full of missing values, fill them with the mean across the stations
    nacols = valuesTable.columns[valuesTable.isna().all(axis=0)]
    if nacols.size > 0:
        valuesTable[nacols] = pd.concat([valuesTable.mean(axis=1)] * nacols.size, axis=1)

    mask = (valuesTable.index > lastWritingDateTime)
    valuesTableNewestTimePeriod = valuesTable.loc[mask]

    # Interpolate over Time if there are new values for the calculation
    if not valuesTableNewestTimePeriod.empty:
        interpolatedPoints = interpolateOverTime(valuesTableNewestTimePeriod, positionTable, tableWithNan, points, objectNames, countPositions,
                                                 nameValue,
                                                 plotMap=plotMap)
        return interpolatedPoints
    else:
        # If there are no new values return None
        return None


def makeAllFLoats(jsonBody):
    for measurement in jsonBody:
        for tag in measurement['tags']:
            if isinstance(measurement['tags'][tag], int):
                measurement['tags'][tag] = float(measurement['tags'][tag])
        for tag in measurement['fields']:
            if isinstance(measurement['fields'][tag], int):
                measurement['fields'][tag] = float(measurement['fields'][tag])


def getRequest(url, headers={}, data={}):
    try:
        request = requests.request("GET", url, headers=headers, data=data)
        return request
    except:
        print("Fehler bei http-GET-Request. url: ", url)
        return None


def readFromInfluxDB(database, query):
    try:
        client = InfluxDBClient(host=database.host.hostAddress, port=database.host.hostPort, username=database.host.hostUsername, \
                                                                                          password=database.host.hostPassword,
                                database=database.name)
        result = client.query(query)
        return result
    except  Exception as error:
        print("Fehler beim Lesen aus der InfluxDB: ", database.name)
        print(error)
        return None


def writeToInfluxDB(database, body):
    makeAllFLoats(body)
    try:
        client = InfluxDBClient(host=database.host.hostAddress, port=database.host.hostPort, username=database.host.hostUsername,
                                password=database.host.hostPassword,
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


def getLastWritingInInfluxDB(database, measurementName, fieldName):
    query = "SELECT {} FROM {} ORDER BY DESC LIMIT 1".format(
        fieldName, measurementName)

    try:
        result = readFromInfluxDB(database, query)
        points = list(result.get_points())
        return points[0]['time']
    except:
        return "2021-01-01T10:00:00.000000Z"


def findMeasurement(measurementList: [InfluxMeasurement], name: str):
    for measurement in measurementList:
        if measurement.name == name:
            return measurement
    return None


def getValuesOfKey(dictList, key: str):
    values = []
    for element in dictList:
        if key in element:
            values.append(element[key])
    # remove duplicates
    return list(set(values))


def writeDataFrameToInfluxDB(database, measurement, dataframe, tags):
    # Throws errors if something goes wrong when writing to influxDB
    try:
        # Create a DataFrame-InfluxDB-Client
        dataframeClient = DataFrameClient(host=database.host.hostAddress, port=database.host.hostPort, username=database.host.hostUsername,
                                          password=database.host.hostPassword,
                                database=database.name)
        # Write points to InfluxDB. Returns True when everything has worked
        if dataframeClient.write_points(dataframe=dataframe, measurement=measurement, tags=tags):
            print("Daten in DB {} geschrieben.".format(database.name))
        else:
            print("Fehler beim Schreiben eines DataFrame in die InfluxDB: ", database.name)
    except Exception as error:
        print("Fehler beim Schreiben eines DataFrame in die InfluxDB: ", database.name)
        # Try to read Error message and code
        try:
            errorContent = json.loads(error.__dict__['content'])
            errorCode = error.__dict__['code']
            print("Code: ", errorCode)
            print("Error: ", errorContent['error'])
        except:
            print(error)


def saveLauraDataToInfluxDB(interpolatedData, database, measurement, parameterName):
    for index, value in interpolatedData.iteritems():
        # Delete index name
        value.index.name = None

        # Define Tags
        tags = {'object': index}
        tags['weather_source'] = 'Meteo'
        if measurement == 'dataPredictionLaura':
            tags['time_at_prediction'] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") + "Z"  # current time in zulu time

        # Create Dataframe to write to influxDB
        influxDataframe = pd.DataFrame({parameterName: value.values}, pd.to_datetime(value.index))

        # Write Dataframe to influxDB
        writeDataFrameToInfluxDB(database, measurement, influxDataframe, tags)


def getDataFromRegressionModel(parameterName: str = 'temperature', sourceMeasurement: str = 'weather', targetMeasurement: str = 'dataLaura',
                               printMapAndGraphs: bool = False, lastWritingInInfluxDB='2021-01-01T10:00:00.000000Z'):
    # Origin of the Data
    sourceDatabase = swaWeatherStorageDatabase

    # Destination of the Data
    targetDatabase = swaLauraDatabase

    # Define calculation points in the zones
    pointOfInterest = list(objects.values())
    objectNames = list(objects.keys())

    if printMapAndGraphs:
        # Get all available Data when print Maps and Graphs
        lastWritingInInfluxDB = "2021-01-01T10:00:00.000000Z"

    # Get source data from InfluxDB
    response = readFromInfluxDB(sourceDatabase, "SELECT * from " + sourceMeasurement)
    if response is None:
        print("Error! Response is empty. Could not read data from Influxdb")
        return
    df = pd.DataFrame.from_dict(response.get_points())  # Create Dataframe out of source data

    # Perform interpolation of the data and determine values for the calculation points
    interpolatedPoints = interpolate(df, pointOfInterest, parameterName=parameterName, objectNames=objectNames, plotMap=printMapAndGraphs,
                                     lastWritingDateTime=lastWritingInInfluxDB)

    # Save determined values for the calculation points into the database
    if interpolatedPoints is not None:
        saveLauraDataToInfluxDB(interpolatedPoints, targetDatabase, targetMeasurement, parameterName)


def getDataFromSensors(parameterName: str = 'temperature', sourceMeasurementName: str = 'weather', targetMeasurement: str = 'dataLaura',
                       printMapAndGraphs: bool = False, lastWritingInInfluxDB="2021-01-01T10:00:00.000000Z"):
    # Origin of the Data
    sourceDatabase = swaLauraDatabase

    sourceMeasurement = sourceDatabase.findMeasurement(sourceMeasurementName)
    # Destination of the Data
    targetDatabase = swaLauraDatabase
    objectNames = list(objects.keys())

    # Get source data from InfluxDB
    parameterNameForSource = sourceMeasurement.fields[sourceMeasurement.mapFields.index(parameterName)]
    for objectName in objectNames:

        response = readFromInfluxDB(sourceDatabase, "SELECT " + parameterNameForSource + " from " +
                                    sourceMeasurementName + " WHERE time > '" + lastWritingInInfluxDB + "' AND object!='" + objectName + "'")
        if response is None:
            print("Error! Response is empty. Could not read data from Influxdb")
            return
        df = pd.DataFrame.from_dict(response.get_points())  # Create Dataframe out of source data
        if df.empty:
            continue
        df = df.rename(columns={parameterNameForSource: parameterName})
        df['time'] = pd.to_datetime(df['time'])
        df = df.set_index('time')
        tags = {'object': objectName, 'weather_source': 'Sensor'}
        writeDataFrameToInfluxDB(targetDatabase, measurement=targetMeasurement, dataframe=df, tags=tags)


##################################################
#       Settings
#############################################

def fillSourcePointValues():
    # fillDataBrunoMeasurement(parameterName='temperature', sourceMeasurement='weather', targetMeasurement='dataBruno', printMapAndGraphs=True)

    # Fill Measurement swaBruno.dataBruno with Data from Measurement swaWeatherStorage.weather
    for parameter in swaWeatherStorageDatabase.findMeasurement(nameOfMeasurement="weather").fields:
        lastWritingInInfluxDB = getLastWritingInInfluxDB(database=swaLauraDatabase, measurementName='dataLaura', fieldName=parameter)
        getDataFromRegressionModel(parameterName=parameter, sourceMeasurement='weather', targetMeasurement='dataLaura',
                                   lastWritingInInfluxDB=lastWritingInInfluxDB)
        getDataFromSensors(parameterName=parameter, sourceMeasurementName='environment', targetMeasurement='dataLaura',
                           lastWritingInInfluxDB=lastWritingInInfluxDB)

    for parameter in swaWeatherStorageDatabase.findMeasurement(nameOfMeasurement="weatherPrediction").fields:
        getDataFromRegressionModel(parameterName=parameter, sourceMeasurement='weatherPrediction', targetMeasurement='dataPredictionLaura')


def setSourceMeasurements():
    weatherMeasurement = InfluxMeasurement(name="weather",
                                           tags=["pos_longitude", "pos_latitude", "source", "time_at_prediction", "time"],
                                           fields=["temperature", "humidity", "solar_radiation", "rain", "wind"])
    swaWeatherStorageDatabase.measurements.append(weatherMeasurement)

    weatherPredictionMeasurement = InfluxMeasurement(name="weatherPrediction",
                                                     tags=["pos_longitude", "pos_latitude", "source", "time_at_prediction", "time"],
                                                     fields=["temperature", "humidity", "solar_radiation", "rain", "wind"])
    swaWeatherStorageDatabase.measurements.append(weatherPredictionMeasurement)

    soilMeasurement = InfluxMeasurement(name="soil", tags=["device_eui", "device_name", "pos_longitude", "pos_latitude", "object"],
                                        fields=["soil_temp", "soil_moisture", "battery_voltage"],
                                        mapFields=["soil_temp", "soil_moisture"])
    swaLauraDatabase.measurements.append(soilMeasurement)
    environmentMeasurement = InfluxMeasurement(name="environment", tags=["device_eui", "device_name", "pos_longitude", "pos_latitude", "object"],
                                               fields=["air_temp", "humidity", "solar_radiation", "rain_presence", "rain_raw", "barometric_pressure",
                                                       "battery_voltage", "wind"],
                                               mapFields=["temperature", "humidity", "solar_radiation", "rain", None, None, None, "wind"])
    swaLauraDatabase.measurements.append(environmentMeasurement)


def setSinkMeasurement():
    dataLauraMeasurement = InfluxMeasurement(name="dataLaura", tags=["object", "weather_source"],
                                             fields=["temperature", "humidity", "solar_radiation", "rain", "wind", "soil_temp", "soil_moisture"])
    swaLauraDatabase.measurements.append(dataLauraMeasurement)
    dataPredictionLauraMeasurement = InfluxMeasurement(name="dataPredictionLaura", tags=["object", "time_at_prediction"],
                                                       fields=["temperature", "humidity", "solar_radiation", "rain", "wind", "soil_temp",
                                                               "soil_moisture"])
    swaLauraDatabase.measurements.append(dataPredictionLauraMeasurement)


if __name__ == '__main__':
    influx_endpoint = getEnvValue('INFLUX_ENDPOINT').split(':')
    influxDBHost = Host(influx_endpoint[1][2:], hostPort=int(influx_endpoint[2]), hostUsername=getEnvValue(
        'INFLUXDB_ADMIN_USER'),
                        hostPassword=getEnvValue('INFLUXDB_ADMIN_PASSWORD'))
    swaLauraDatabase = InfluxDatabase(name="swaLauraTest", measurements=[], host=influxDBHost)
    swaWeatherStorageDatabase = InfluxDatabase(name="swaWeatherStorageTest", measurements=[], host=influxDBHost)

    objects = readMapFile('object_definition.json')
    setSourceMeasurements()
    setSinkMeasurement()
    fillSourcePointValues()
