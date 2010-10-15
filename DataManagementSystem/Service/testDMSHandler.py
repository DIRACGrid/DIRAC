########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/DataManagementSystem/Service/DataIntegrityHandler.py $
########################################################################
__RCSID__   = "$Id: DataIntegrityHandler.py 28966 2010-10-05 13:24:36Z acsmith $"
__VERSION__ = "$Revision: 1.8 $"

from types                                      import *
from DIRAC                                      import gLogger, gConfig, rootPath, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler            import RequestHandler
from DIRAC.DataManagementSystem.DB.testDMSDB    import testDMSDB

# This is a global instance of the DataIntegrityDB class
testDMSDB = False

def initializetestDMSDB(serviceInfo):
  global testDMSDB
  testDMSDB = testDMSDB()
  return S_OK()

class testDMSHandler(RequestHandler):

   types_insertSomething = [StringTypes,[IntType,LongType]]
   def export_insertSomething(self,user,files):
     return testDMSDB.insertSomething(user,files)

   types_querySomething = [StringTypes]
   def export_querySomething(user):
     return testDMSDB.querySomething(user)
