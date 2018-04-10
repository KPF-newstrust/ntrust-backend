
class EntityCollector:
    entity_dic = dict()

    @staticmethod
    def load_data(mgodb):
        coll = mgodb.entity_dic
        docs = coll.find(None, {"word": True, "types": True, "_id": False})
        for doc in docs:
            EntityCollector.entity_dic[doc["word"]] = doc["types"]
        print("EntityCollector: %d words loaded" % (len(EntityCollector.entity_dic)))
        pass

    def __init__(self):
        if len(EntityCollector.entity_dic) == 0:
            raise Exception("Entity dictionary data is not loaded yet.")

        self.allPersons = []
        self.allOrgans = []
        self.allLocations = []
        self.allPlans = []
        self.allProducts = []
        self.allEvents = []

    def feed(self, word):
        if word not in EntityCollector.entity_dic:
            return
        types = EntityCollector.entity_dic[word]
        for type in types:
            if type == "PS":
                self.allPersons.append(word)
            elif type == "OG":
                self.allOrgans.append(word)
            elif type == "LC":
                self.allLocations.append(word)
            elif type == "PL":
                self.allPlans.append(word)
            elif type == "PR":
                self.allProducts.append(word)
            elif type == "EV":
                self.allEvents.append(word)
            else:
                raise Exception("Invalid type code: " + type)

    def get_result(self, prefix, ret=None):
        if ret is None:
            ret = dict()
        ret[prefix + "PS"] = list(set(self.allPersons)) or None
        ret[prefix + "OG"] = list(set(self.allOrgans)) or None
        ret[prefix + "LC"] = list(set(self.allLocations)) or None
        ret[prefix + "PL"] = list(set(self.allPlans)) or None
        ret[prefix + "PR"] = list(set(self.allProducts)) or None
        ret[prefix + "EV"] = list(set(self.allEvents)) or None
        return ret

    def dump(self):
        print("Persons:", set(self.allPersons))
        print("Organizations:", set(self.allOrgans))
        print("Locations:", set(self.allLocations))
        print("Plans:", set(self.allPlans))
        print("Products:", set(self.allProducts))
        print("Events:", set(self.allEvents))
