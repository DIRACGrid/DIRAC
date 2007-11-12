import DIRAC

class RRDManager:

  def __init__(self):
    self.dbLocation = "%s/data/monitoring/activities" % DIRAC.rootPath
    self.acDescriptions = {}


gRRDManager = RRDManager()