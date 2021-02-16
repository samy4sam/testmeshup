class Host:

    def __init__(self, hostAddress: str, hostPort: int, hostUsername: str, hostPassword: str):
        self.hostAddress = hostAddress
        self.hostPort = hostPort
        self.hostUsername = hostUsername
        self.hostPassword = hostPassword


class InfluxDatabase:

    def __init__(self, name, host: Host):
        self.name = name
        self.host = host.hostAddress
        self.port = host.hostPort
        self.username = host.hostUsername
        self.password = host.hostPassword