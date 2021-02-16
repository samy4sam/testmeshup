class Datapoint:
    needsFilter = False
    needsLoop = False

    def __init__(self, name, pathToValue=None, value=None):
        self.name = name
        self.pathToValue = pathToValue
        self.value = value


class Filter:
    def __init__(self, filterValues, pathToFilter=None):
        self.pathToFilter = pathToFilter
        self.filterValues = filterValues


class Datasource:
    loopPath = None
    time = ""

    def __init__(self, fields, tags, filter=None):
        self.fields = fields
        self.tags = tags
        self.filter = filter

    # dictFieldValues = [{'name': ... , 'value': ...}, ...]
    def writeFields(self, listFieldValues):
        for fieldValue in listFieldValues:
            nameOfField = fieldValue['name']
            valueOfField = fieldValue['value']
            indexOfFieldInArray = next((index for (index, d) in enumerate(self.fields) if d["name"] == nameOfField),
                                       None)
            self.fields[indexOfFieldInArray].writeValue = valueOfField

    def writeTags(self, listTagValues):
        for tagValue in listTagValues:
            nameOfTag = tagValue['name']
            valueOfTag = tagValue['value']
            indexOfFieldInArray = next((index for (index, d) in enumerate(self.fields) if d["name"] == nameOfTag), None)
            self.tags[indexOfFieldInArray].writeValue = valueOfTag

    def setLoopPath(self, path):
        self.loopPath = path

    def setTime(self, time):
        self.time = time


class UrlDatasource(Datasource):
    def __init__(self, fields, tags, url, filter=None):
        super().__init__(fields, tags, filter)
        self.url = url


class ElementDatasource(Datasource):
    def __init__(self, fields, tags, token, filter=None):
        super().__init__(fields, tags, filter)
        self.token = token


class Source:
    def __init__(self, name, protocol, datasources):
        self.name = name
        self.protocol = protocol
        self.datasources = datasources


class Measurement:
    packetsForInflux = []

    def __init__(self, name, sources):
        self.name = name
        self.sources = sources


class PacketForInflux:
    time = None

    def __init__(self, tags, fields):
        self.tags = tags
        self.fields = fields


class Host:

    def __init__(self, hostAddress: str, hostPort: int, hostUsername: str, hostPassword: str):
        self.hostAddress = hostAddress
        self.hostPort = hostPort
        self.hostUsername = hostUsername
        self.hostPassword = hostPassword


class Database:

    def __init__(self, name, measurements, host: Host):
        self.name = name
        self.measurements = measurements
        self.host = host.hostAddress
        self.port = host.hostPort
        self.username = host.hostUsername
        self.password = host.hostPassword
