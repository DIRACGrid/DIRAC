

import GSI

requiredGSIVersion = "0.4.0"
if GSI.version.__version__ < requiredGSIVersion:
  raise Exception( "pyGSI is not the latest version (installed %s required %s)" % ( GSI.version.__version__, requiredGSIVersion ) )

GSI.SSL.set_thread_safe()

nid = GSI.crypto.create_oid( "1.2.42.42", "diracGroup", "DIRAC group" )
GSI.crypto.add_x509_extension_alias( nid, 78 ) #Alias to netscape comment, text based extension
nid = GSI.crypto.create_oid( "1.3.6.1.4.1.8005.100.100.5", "vomsExtensions", "VOMS extension" )
GSI.crypto.add_x509_extension_alias( nid, 78 ) #Alias to netscape comment, text based extension

import DIRAC.Core.Security.Locations
import DIRAC.Core.Security.CS
import DIRAC.Core.Security.File
import DIRAC.Core.Security.Properties