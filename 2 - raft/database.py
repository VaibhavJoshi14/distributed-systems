class Database:
    def __init__(self, db_file):
        self.db_file = db_file
        self.data = self.__load_file()

    def __load_file(self):
        try:
            data = {}
            with open(self.db_file, 'r') as f:
                lines = f.read().split("\n")
                for l in lines:
                    if (len(l) <= 1):
                        continue
                    [k, v] = l.split()
                    data[k] = v
            return data
        except FileNotFoundError:
            return {}

    def store(self, key, value):
        self.data[key] = value
        with open(self.db_file, 'a') as f:
            f.write(key + " " + value + "\n")  

    def get(self, key):
        return self.data.get(key, None)

if __name__ == '__main__':
    db = Database("log.txt")
    print(db.data)
    db.store("h1", "v1")
    db.store("h2", "v2")
    db.store("kq", "lmng")
    print(db.data)
    print(db.get("kq"))

