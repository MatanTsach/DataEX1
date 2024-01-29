from cassandra.cluster import Cluster

class CassandraConnection:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None
        self.cluster = None

    def connect(self):
        self.cluster = Cluster([self.host], port=int(self.port))
        self.session = self.cluster.connect()

        return self.session
    
    def set_keyspace(self, keyspace):
        self.session.set_keyspace(keyspace)
    
    def execute_cql(self, cql_script):
        with open(cql_script, "r") as file:
            cql_commands = file.read()
            for command in cql_commands.split(";"):
                command = command.strip()  # Remove leading/trailing whitespace
                if command:
                    self.session.execute(command)

    def close(self):
        self.cluster.shutdown()
