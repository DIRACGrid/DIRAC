#!/bin/env python

""" dummy testing for RequestContainer 

:deprecated:
"""


import xml.dom.minidom 
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer 

def getRequest( operation ):
  """ fake requestDict 

  :param str operation: sub-request operation attribute 
  """ 
  requestContainer = RequestContainer( init = False )
  requestContainer.setJobID( 1 )
  requestContainer.setOwnerDN( "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cibak/CN=605919/CN=Krzysztof Ciba" )
  requestContainer.setOwnerGroup( "lhcb_user" )
  requestContainer.setDIRACSetup( "LHCb-Production" )
  requestContainer.setSourceComponent( None )
  requestContainer.setCreationTime( "0000-00-00 00:00:00" )
  requestContainer.setLastUpdate( "2011-12-01 04:57:02" )
  requestContainer.setStatus( "Waiting" )
  requestContainer.setAttribute( "RequestID", 123456789  )
  requestContainer.initiateSubRequest( "transfer" )
  subRequestDict = { "Status" : "Waiting", 
                     "SubRequestID"  : 2222222, 
                     "Operation" : operation, 
                     "Arguments" : None,
                     "ExecutionOrder" : 0, 
                     "SourceSE" : None, 
                     "TargetSE" : "CERN-USER,PIC-USER",
                     "Catalogue" : "LcgFileCatalogCombined", 
                     "CreationTime" : "2011-12-01 04:57:02", 
                     "SubmissionTime" : "2011-12-01 04:57:02",
                     "LastUpdate" : "2011-12-01 20:14:22" }
  requestContainer.setSubRequestAttributes( 0, "transfer", subRequestDict )  
  files =  [ { "FileID" : 3333333, 
               "LFN" : "/lhcb/user/c/cibak/11889/11889410/test.zzz", 
               "Size" : 44444444, 
               "PFN" : "srm://srm-lhcb.gridpp.rl.ac.uk/castor/ads.rl.ac.uk/prod/lhcb/user/c/cibak/11889/11889410/test.zzz", 
               "GUID" : "5P13RD4L-4J5L-3D21-U5P1-3RD4L4J5P13R", 
               "Md5" : None, 
               "Addler" : "92b85e26", 
               "Attempt" : 1, 
               "Status" : "Waiting" } ]    
  requestContainer.setSubRequestFiles( 0, "transfer", files )
  return requestContainer

if __name__ == "__main__":

  req = getRequest( "replicateAndRegister" )
    
  xmlDoc_NEW = req.toXML_new( "transfer" )["Value"]
  xmlDoc_OLD = req.toXML( "transfer" )["Value"]

  xmlDoc_OLD = xml.dom.minidom.parseString( xmlDoc_OLD )
  xmlDoc_OLD.normalize()
  xmlDoc_OLD = xmlDoc_OLD.toxml()
  xmlDoc_NEW = xml.dom.minidom.parseString( xmlDoc_NEW ).toxml()

  print len(xmlDoc_OLD)

  print len(xmlDoc_NEW)
  print xmlDoc_NEW


