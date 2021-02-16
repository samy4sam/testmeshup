class InfluxDatapoint:
    def __init__(self, name, pathToValue=None, value=None):
        self.name = name
        self.value = value

class InfluxMeasurement:

    def __init__(self, name: str, tags: [str], fields: [str]):
        self.name = name
        self.tags = tags
        self.fields = fields


class Host:

    def __init__(self, hostAddress: str, hostPort: int, hostUsername: str, hostPassword: str):
        self.hostAddress = hostAddress
        self.hostPort = hostPort
        self.hostUsername = hostUsername
        self.hostPassword = hostPassword


class InfluxDatabase:

    def __init__(self, name, measurements: [InfluxMeasurement], host: Host):
        self.name = name
        self.measurements = measurements
        self.host = host.hostAddress
        self.port = host.hostPort
        self.username = host.hostUsername
        self.password = host.hostPassword

    def findMeasurement(self, nameOfMeasurement: str):
        for measurement in self.measurements:
            if measurement.name == nameOfMeasurement:
                return measurement
        return None