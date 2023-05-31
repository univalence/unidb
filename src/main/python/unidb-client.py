import json
import socket


class RemoteStoreSpace(object):
    def __init__(self, name: str, host: str, port: int):
        self.__host = host
        self.__port = port
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.name = name

    def getStore(self, tableName: str):
        return RemoteStore(tableName, self)

    def createStore(self, tableName: str):
        self.send(b"CREATESTORE %b" % bytes(tableName, "utf-8"))
        self.getStore(tableName)

    def send(self, request: bytes):
        bytesSent = 0
        while bytesSent < len(request):
            sent = self.__socket.send(request[bytesSent:])
            if sent == 0:
                raise RuntimeError("socket connection broken")
            bytesSent += sent

        if request.strip().upper() != b"CLOSE":
            data = self.__socket.recv(4096)
            if data == b"":
                raise RuntimeError("socket connection broken")
            return data

    def __enter__(self):
        self.__socket.__enter__()
        self.__socket.connect((self.__host, self.__port))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.send(b"CLOSE")
        self.__socket.shutdown(socket.SHUT_RDWR)
        self.__socket.__exit__(exc_type, exc_val, exc_tb)

    def close(self):
        self.send(b"CLOSE")
        self.__socket.shutdown(socket.SHUT_RDWR)
        self.__socket.close()


class RemoteStore(object):
    def __init__(self, name: str, rdb: RemoteStoreSpace):
        self.__name = name
        self.__rdb = rdb
        self.__storeSpaceName = rdb.name
        self.__fullName = name + "." + rdb.name

    def get(self, key: str):
        data = self.__rdb.send(b"GET %b %b" % (bytes(self.__fullName, "utf-8"), bytes(key, "utf-8")))
        response = json.loads(data)
        if response['status'] == 'KO':
            raise RuntimeError(response["value"])
        else:
            return response['value']

    def getPrefix(self, prefix: str):
        data = self.__rdb.send(b"GETPREFIX %b %b" % (bytes(self.__fullName, "utf-8"), bytes(prefix, "utf-8")))
        response = json.loads(data)
        if response['status'] == 'KO':
            raise RuntimeError(response["value"])
        else:
            return response['value']

    def getFrom(self, key: str):
        data = self.__rdb.send(b"GETFROM %b %b" % (bytes(self.__fullName, "utf-8"), bytes(key, "utf-8")))
        response = json.loads(data)
        if response['status'] == 'KO':
            raise RuntimeError(response["value"])
        else:
            return response['value']

    def put(self, key: str, value):
        v = json.dumps(value)
        data = self.__rdb.send(b"PUT %b %b %b" % (bytes(self.__fullName, "utf-8"), bytes(key, "utf-8"), bytes(v, "utf-8")))
        response = json.loads(data)
        if response['status'] == 'KO':
            raise RuntimeError(response["value"])
        else:
            return response['value']

    def scan(self):
        data = self.__rdb.send(b"GETALL %b" % bytes(self.__fullName, "utf-8"))
        response = json.loads(data)
        if response['status'] == 'KO':
            raise RuntimeError(response["value"])
        else:
            return response['value']


if __name__ == "__main__":
    with RemoteStoreSpace("rdb", "localhost", 19040) as db:
        table = db.getStore("table")
        print(table.scan())
        print(table.get('123'))
        print(table.getPrefix('123#'))
