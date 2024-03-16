# a database class which stores (key, value) type of data in storage.
class DatabaseKV:
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
        # if key not in dict, then store in file, else update the corresponding value in file
        if key not in self.data:
            with open(self.db_file, 'a') as f:
                f.write(key + " " + value + "\n") 

        else:
            # read the old file
            with open(self.db_file, 'r') as f:
                data = f.read()
            # replace the value corresponding to key in data read
            old = key + " " + self.data[key] + "\n"
            new =  key + " " + value + "\n"
            data = data.replace(old, new)

            #write the data (full write to file)
            with open(self.db_file, 'w') as f:
                f.write(data)
        
        self.data[key] = value

    def get(self, key):
        return self.data.get(key, None)

if __name__ == '__main__':
    db = DatabaseKV("log.txt")
    print(db.data)
    db.store("h1", "v1")
    db.store("h2", "qq")
    db.store("kq", "lmng")
    print(db.data)
    print(db.get("kq"))
    db.store("hello", "hi")
    print(db.data)

