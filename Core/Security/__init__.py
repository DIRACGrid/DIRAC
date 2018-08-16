# $HeadURL$
__RCSID__ = "$Id$"

import GSI
import os

requiredGSIVersion = "0.6.3"
if GSI.version.__version__ < requiredGSIVersion:
  raise Exception( "pyGSI is not the latest version (installed %s required %s)" % ( GSI.version.__version__, requiredGSIVersion ) )

GSI.SSL.set_thread_safe()

nid = GSI.crypto.create_oid( "1.2.42.42", "diracGroup", "DIRAC group" )
GSI.crypto.add_x509_extension_alias( nid, 78 ) #Alias to netscape comment, text based extension


# USE_TORNADO_IOLOOP is defined in tornado starting script, you don't have to care about it
# it's defined when tornado is running
if not os.environ.get('USE_TORNADO_IOLOOP', 'false').lower() == 'true':
  ########### WARNING ###########
  # 
  # I don't really know how PyGSI and M2Crypto works, These lines initialise some variables for
  # PyGSI so you need them, but with Tornado (and M2Crypto) it won't work. (it's why there is "if")
  # For devellopement it work like this, for production you may migrate to M2Crypto before
  # migrate to Tornado who use M2Crypto
  #
  # Maybe it's better not to merge this code with integration branch while M2Crypto is not implemented
  # or while Tornado (and his M2Crypto version) is not merge too.
  #
  ################################
  nid = GSI.crypto.create_oid( "1.3.6.1.4.1.8005.100.100.5", "vomsExtensions", "VOMS extension" )
  GSI.crypto.add_x509_extension_alias( nid, 78 ) #Alias to netscape comment, text based extension
  nid = GSI.crypto.create_oid( "1.3.6.1.4.1.8005.100.100.11", "vomsAttribute", "VOMS attribute" )
  GSI.crypto.add_x509_extension_alias( nid, 78 ) #Alias to netscape comment, text based extension
