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
swaBrunoDatabase = None
swaWeatherStorageDatabase = None

def getEnvValue(key):
    pathToEnv = Path(str(Path(__file__).parents[3])+'\.env')
    load_dotenv(pathToEnv.resolve())
    return os.getenv(key)

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


def interpolateOverTime(valuesTable, positionTable, tableWithNan, points, zones, countPositions, nameValue, plotMap):
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
            listWithColumnNames.append(zones[i])
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


def interpolate(df, points: [int], parameterName: str, zone: [str], plotMap: bool, lastWritingDateTime):
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
        interpolatedPoints = interpolateOverTime(valuesTableNewestTimePeriod, positionTable, tableWithNan, points, zone, countPositions, nameValue,
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
        client = InfluxDBClient(host=database.host, port=database.port, username=database.username, password=database.password,
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
        dataframeClient = DataFrameClient(host=database.host, port=database.port, username=database.username, password=database.password,
                                          database=database.name)
        # Write points to InfluxDB. Returns True when everything has worked
        if dataframeClient.write_points(dataframe=dataframe, measurement=measurement, tags=tags):
            print("Daten in DB {} geschrieben.".format(database.name))
        else:
            print("Fehler beim Schreiben in die InfluxDB: ", database.name)
    except Exception as error:
        print("Fehler beim Schreiben in die InfluxDB: ", database.name)
        # Try to read Error message and code
        try:
            errorContent = json.loads(error.__dict__['content'])
            errorCode = error.__dict__['code']
            print("Code: ", errorCode)
            print("Error: ", errorContent['error'])
        except:
            print(error)


def saveBrunoDataToInfluxDB(interpolatedData, database, measurement, parameterName):
    for index, value in interpolatedData.iteritems():
        # Delete index name
        value.index.name = None

        # Define Tags
        tags = {'zone': index}
        if measurement == 'dataPredictionBruno':
            tags['time_at_prediction'] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") + "Z"  # current time in zulu time

        # Create Dataframe to write to influxDB
        influxDataframe = pd.DataFrame({parameterName: value.values}, pd.to_datetime(value.index))

        # Write Dataframe to influxDB
        writeDataFrameToInfluxDB(database, measurement, influxDataframe, tags)



def fillDataBrunoMeasurement(parameterName: str = 'temperature', sourceMeasurement: str = 'weather', targetMeasurement: str = 'dataBruno',
                             printMapAndGraphs: bool = False):
    # Origin of the Data
    sourceDatabase = swaWeatherStorageDatabase

    # Destination of the Data
    targetDatabase = swaBrunoDatabase

    # Define calculation points in the zones
    pointOfInterest = [[47.5455, 9.2943], [47.5995, 9.2869], [47.6098, 9.2573], [47.5941, 9.2470]]
    zoneNames = ['Amriswil', 'Güttingen', 'Altnau', 'Langrickenbach']

    if printMapAndGraphs:
        # Get all available Data when print Maps and Graphs
        lastWritingInInfluxDB = "2021-01-01T10:00:00.000000Z"
    else:
        # Get DateTime of the newest data package in the target DB
        lastWritingInInfluxDB = getLastWritingInInfluxDB(database=targetDatabase, measurementName=targetMeasurement, fieldName=parameterName)

    # Get source data from InfluxDB
    response = readFromInfluxDB(sourceDatabase, "SELECT * from " + sourceMeasurement)
    df = pd.DataFrame.from_dict(response.get_points())  # Create Dataframe out of source data

    # Perform interpolation of the data and determine values for the calculation points
    interpolatedPoints = interpolate(df, pointOfInterest, parameterName=parameterName, zone=zoneNames, plotMap=printMapAndGraphs,
                                     lastWritingDateTime=lastWritingInInfluxDB)
    # Plot Graph with Values over time
    if printMapAndGraphs and interpolatedPoints is not None:
        plotValuesOverTime(interpolatedPoints=interpolatedPoints, valueName=parameterName, pointNames=zoneNames)

    # Save determined values for the calculation points into the database
    if interpolatedPoints is not None:
        saveBrunoDataToInfluxDB(interpolatedPoints, targetDatabase, targetMeasurement, parameterName)


##################################################
#       Settings
#############################################

def fillSourcePointValues():
    #fillDataBrunoMeasurement(parameterName='temperature', sourceMeasurement='weather', targetMeasurement='dataBruno', printMapAndGraphs=True)

    # Fill Measurement swaBruno.dataBruno with Data from Measurement swaWeatherStorage.weather
    for parameter in swaWeatherStorageDatabase.findMeasurement(nameOfMeasurement="weather").fields:
        fillDataBrunoMeasurement(parameterName=parameter, sourceMeasurement='weather', targetMeasurement='dataBruno')

    for parameter in swaWeatherStorageDatabase.findMeasurement(nameOfMeasurement="weatherPrediction").fields:
        fillDataBrunoMeasurement(parameterName=parameter, sourceMeasurement='weatherPrediction', targetMeasurement='dataPredictionBruno')


def setSourceMeasurements():
    weatherMeasurement = InfluxMeasurement(name="weather",
                                           tags=["pos_longitude", "pos_latitude", "source", "time_at_prediction", "time"],
                                           fields=["temperature", "humidity", "solar_radiation", "rain", "wind", "barometric_pressure", "dew_point"])
    swaWeatherStorageDatabase.measurements.append(weatherMeasurement)

    weatherPredictionMeasurement = InfluxMeasurement(name="weatherPrediction",
                                                     tags=["pos_longitude", "pos_latitude", "source", "time_at_prediction", "time"],
                                                     fields=["temperature", "humidity", "solar_radiation", "rain", "wind"])
    swaWeatherStorageDatabase.measurements.append(weatherPredictionMeasurement)

    dataREAMeasurement = InfluxMeasurement(name="dataREA", tags=["zone"], fields=["consumption"])
    swaBrunoDatabase.measurements.append(dataREAMeasurement)


def setSinkMeasurement():
    dataBrunoMeasurement = InfluxMeasurement(name="dataBruno", tags=["zone"],
                                             fields=["temperature", "humidity", "solar_radiation", "rain", "consumption", "wind"])
    swaBrunoDatabase.measurements.append(dataBrunoMeasurement)
    dataPredictionBrunoMeasurement = InfluxMeasurement(name="dataPredictionBruno", tags=["zone", "time_at_prediction"],
                                                       fields=["temperature", "humidity", "solar_radiation", "rain", "wind"])
    swaBrunoDatabase.measurements.append(dataPredictionBrunoMeasurement)


if __name__ == '__main__':
    influx_endpoint = getEnvValue('INFLUX_ENDPOINT').split(':')
    influxDBHost = Host(hostAddress="localhost", hostPort=int(influx_endpoint[2]), hostUsername=getEnvValue('INFLUXDB_ADMIN_USER'),
                        hostPassword=getEnvValue('INFLUXDB_ADMIN_PASSWORD'))
    swaBrunoDatabase = InfluxDatabase(name="swaBruno", measurements=[], host=influxDBHost)
    swaWeatherStorageDatabase = InfluxDatabase(name="swaWeatherStorageTest", measurements=[], host=influxDBHost)

    zonen = readMapFile('zone_mapping.json')

    setSourceMeasurements()
    setSinkMeasurement()
    fillSourcePointValues()
