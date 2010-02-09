import sys
#import unittest
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
from DIRAC.ResourceStatusSystem.PolicySystem.PDP import PDP

import DIRAC.ResourceStatusSystem.test.fake_rsDB

from DIRAC.Core.Base import Script
Script.parseCommandLine() 

#sito = {'name':'LCG.IFJ-PAN.pl', 'siteType':'T2'} #OK
#sito = {'name':'LCG.CERN.ch', 'siteType':'T0'} #OK
sito = {'name':'LCG.NIKHEF.nl', 'siteType':'T1'} #OK
servizio = {'name':'Computing@LCG.CESGA.es', 'siteType':'T2', 'serviceType':'Computing'} #OK
risorsa = {'name':'lcgce01.jinr.ru', 'siteType':'T2', 'resourceType':'CE'} #OK
#risorsa = {'name':'ce125.cern.ch', 'siteType':'T0', 'resourceType':'CE'} #OK
#risorsa = {'name':'cream-1-fzk.gridka.de', 'siteType':'T1', 'resourceType':'CE'} #OK
#risorsa = {'name':'bocecream.bo.infn.it', 'siteType':'T1', 'resourceType':'CREAMCE'} #OK
risorsa2 = {'name':'srm.grid.sara.nl', 'siteType':'T1', 'resourceType':'SE'} #OK
risorsa3 = {'name':'lfclhcb.pic.es', 'siteType':'T1', 'resourceType':'LFC_L'} #OK
#risorsa3 = {'name':'prod-lfc-lhcb-central.cern.ch', 'siteType':'T1', 'resourceType':'LFC_C'} #OK
#se = {'name':'CERN_MC_M-DST', 'siteType':'T0'} #OK
se = {'name':'NIKHEF-USER', 'siteType':'T1'} #OK


#sito = {'name':'LCG.CERN2.ch', 'siteType':'T0'} #WRONG
#servizio = {'name':'Computing@LCG.#Ferrara.it', 'siteType':'T2', 'serviceType':'Computing'} #WRONG
#risorsa = {'name':'ce106.pic.es', 'siteType':'T1', 'resourceType':'CE'} #WRONG
#risorsa2 = {'name':'srm-lhcb.cern.ch#', 'siteType':'T0', 'resourceType':'SE'} #WRONG
#se = {'name':'CERN_MC_M-DST#', 'siteType':'T0'} #WRONG

print "\n\n ~~~~~~~ SITO ~~~~~~~ %s \n" %(sito)

for status in ValidStatus:
  for oldStatus in ValidStatus:
    if status == oldStatus:
      continue
    print "############################"
    print " "
    print 'nel test:', status, oldStatus
    pdp = PDP(granularity = 'Site', name = sito['name'], status = status, 
              formerStatus = oldStatus, reason = 'XXXXX', siteType = sito['siteType']) 
    res = pdp.takeDecision()
    print res

print "\n\n ~~~~~~~ SERVICE ~~~~~~~ : %s \n " %servizio

for status in ValidStatus:
  for oldStatus in ValidStatus:
    if status == oldStatus:
      continue
    print "############################"
    print " "
    print 'nel test:', status, oldStatus
    pdp = PDP(granularity = 'Service', name = servizio['name'], status = status, 
              formerStatus = oldStatus, reason = 'XXXXX', siteType = servizio['siteType'],
              serviceType = servizio['serviceType']) 
    res = pdp.takeDecision()
    print res



print "\n\n ~~~~~~~ RISORSA 1 ~~~~~~~ : %s \n " %risorsa

for status in ValidStatus:
  for oldStatus in ValidStatus:
    if status == oldStatus:
      continue
    print "############################"
    print " "
    print status, oldStatus
    pdp = PDP(granularity = 'Resource', name = risorsa['name'], status = status, 
              formerStatus = oldStatus, reason = 'XXXXX', siteType = risorsa['siteType'], 
              resourceType = risorsa['resourceType'])
    res = pdp.takeDecision()
    print res

print "\n\n ~~~~~~~ RISORSA 2 ~~~~~~~ : %s \n " %risorsa2

for status in ValidStatus:
  for oldStatus in ValidStatus:
    if status == oldStatus:
      continue
    print "############################"
    print " "
    print status, oldStatus
    pdp = PDP(granularity = 'Resource', name = risorsa2['name'], status = status, 
              formerStatus = oldStatus, reason = 'XXXXX', siteType = risorsa2['siteType'], 
              resourceType = risorsa2['resourceType'])
    res = pdp.takeDecision()
    print res

print "\n\n ~~~~~~~ RISORSA 3 ~~~~~~~ : %s \n " %risorsa3

for status in ValidStatus:
  for oldStatus in ValidStatus:
    if status == oldStatus:
      continue
    print "############################"
    print " "
    print status, oldStatus
    pdp = PDP(granularity = 'Resource', name = risorsa3['name'], status = status, 
              formerStatus = oldStatus, reason = 'XXXXX', siteType = risorsa3['siteType'], 
              resourceType = risorsa3['resourceType'])
    res = pdp.takeDecision()
    print res

print "\n\n ~~~~~~~ StorageElement ~~~~~~~ : %s \n " %se

for status in ValidStatus:
  for oldStatus in ValidStatus:
    if status == oldStatus:
      continue
    print "############################"
    print " "
    print status, oldStatus
    pdp = PDP(granularity = 'StorageElement', name = risorsa['name'], status = status, 
              formerStatus = oldStatus, reason = 'XXXXX', siteType = risorsa['siteType'], 
              resourceType = risorsa['resourceType']) 
    res = pdp.takeDecision()
    print res
