import json

class Location:
    def __init__(self, crs, tiploc, name, toc):
        self.crs = crs
        self.tiploc = tiploc
        self.name = name
        self.toc = toc

    def __str__(self):
        return "{} - {} - {} - {}".format(self.crs, self.tiploc, self.toc, self.name)

    def __repr__(self):
        return self.__str__()

class LocationMapper:
    def __init__(self, locations_file_path):
        with open(locations_file_path) as f:
            locations = json.loads(f.read())
            self.location_map = {}
            for l in locations["locations"]:
                if "tiploc" in l and "crs" in l and "toc" in l and "name" in l:
                    nl = Location(l["crs"], l["tiploc"], l["name"], l["toc"])
                    self.location_map[nl.tiploc] = nl

    def get_crs(self, tiploc):
        return self.location_map[tiploc].crs

    def get_name(self, tiploc):
        return self.location_map[tiploc].name

if __name__ == "__main__":
    lm = LocationMapper("../national-rail-stations/stations.json")
    print("PLYMTH -> {} [{}].".format(lm.get_crs("PLYMTH"), lm.get_name("PLYMTH")))


