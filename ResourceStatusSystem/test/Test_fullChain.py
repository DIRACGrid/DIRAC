#################################################################################
## $HeadURL $
#################################################################################
#__RCSID__  = "$Id$"
#
#import sys
#
#from DIRAC.Core.Base import Script
#Script.parseCommandLine()
#
#from DIRAC.ResourceStatusSystem.Utilities.Utils  import *
#from DIRAC.ResourceStatusSystem                  import *
#from DIRAC.ResourceStatusSystem.PolicySystem.PDP import PDP
#from DIRAC                                       import gConfig
#import DIRAC.ResourceStatusSystem.test.fake_rsDB
#
#VO = gConfig.getValue("DIRAC/Extensions")
#if 'LHCb' in VO:
#  VO = 'LHCb'
#
#sito = {'name':'LCG.UKI-LT2-QMUL.uk', 'siteType':'T2'} #OK
##sito = {'name':'LCG.CERN.ch', 'siteType':'T0'} #OK
##sito = {'name':'LCG.CNAF.it', 'siteType':'T1'} #OK
#servizio = {'name':'Storage@LCG.GRIDKA.de', 'siteType':'T2', 'serviceType':'Storage'} #OK
#servizio2 = {'name':'Computing@LCG.DORTMUND.de', 'siteType':'T2', 'serviceType':'Computing'} #OK
#servizio3 = {'name':'VO-BOX@LCG.GRIDKA.de', 'siteType':'T1', 'serviceType':'VO-BOX'} #OK
#servizio4 = {'name':'VOMS@LCG.CERN.ch', 'siteType':'T0', 'serviceType':'VOMS'} #OK
#risorsa = {'name':'gridgate.cs.tcd.ie', 'siteType':'T2', 'resourceType':'CE'} #OK
##risorsa = {'name':'ce111.cern.ch', 'siteType':'T0', 'resourceType':'CE'} #OK
##risorsa3 = {'name':'tblb01.nipne.ro', 'siteType':'T2', 'resourceType':'CE'} #OK
#risorsa4 = {'name':'fts.cr.cnaf.infn.it', 'siteType':'T1', 'resourceType':'FTS'} #OK
#risorsa5 = {'name':'voms.cern.ch', 'siteType':'T0', 'resourceType':'VOMS'} #OK
##risorsa = {'name':'hepgrid3.ph.liv.ac.uk', 'siteType':'T1', 'resourceType':'CE'} #OK, ma messo CE al posto di CREAMCE
#risorsa2 = {'name':'ccsrm.in2p3.fr', 'siteType':'T1', 'resourceType':'SE'} #OK
#risorsa3 = {'name':'lhcb-lfc.gridpp.rl.ac.uk', 'siteType':'T1', 'resourceType':'LFC_L'} #OK
##risorsa = {'name':'ce.gina.sara.nl', 'siteType':'T2', 'resourceType':'CE'} #OK
##risorsa3 = {'name':'prod-lfc-lhcb-central.cern.ch', 'siteType':'T1', 'resourceType':'LFC_C'} #OK
#se = {'name':'CERN-RAW', 'siteType':'T0'} #OK
##se = {'name':'PIC_MC_M-DST', 'siteType':'T1'} #OK
#
#useNewRes = False
#
##sito = {'name':'LCG.CERN2.ch', 'siteType':'T0'} #WRONG
##servizio = {'name':'Computing@LCG.#Ferrara.it', 'siteType':'T2', 'serviceType':'Computing'} #WRONG
##risorsa = {'name':'ce106.pic.es', 'siteType':'T1', 'resourceType':'CE'} #WRONG
##risorsa2 = {'name':'srm-lhcb.cern.ch#', 'siteType':'T0', 'resourceType':'SE'} #WRONG
##se = {'name':'CERN_MC_M-DST#', 'siteType':'T0'} #WRONG
#
#print "\n\n ~~~~~~~ SITO ~~~~~~~ %s \n" %(sito)
#
#for status in ValidStatus:
##  for oldStatus in ValidStatus:
##  if status == oldStatus:
##    continue
#  print "############################"
#  print " "
#  print 'dans le test:', status#, oldStatus
#  pdp = PDP(VO, granularity = 'Site', name = sito['name'], status = status,
##            formerStatus = oldStatus,
#            reason = 'XXXXX', siteType = sito['siteType'],
#             useNewRes = useNewRes
#            )
#  res = pdp.takeDecision()
#  print res
#
##print "\n\n ~~~~~~~ SERVICE 1 ~~~~~~~ : %s \n " %servizio
##
##for status in ValidStatus:
###  for oldStatus in ValidStatus:
###    if status == oldStatus:
###      continue
##  print "############################"
##  print " "
##  print 'nel test:', status#, oldStatus
##  pdp = PDP(VO, granularity = 'Service', name = servizio['name'], status = status,
###            formerStatus = oldStatus,
##            reason = 'XXXXX', siteType = servizio['siteType'],
##            serviceType = servizio['serviceType'],
##            useNewRes = useNewRes
##            )
##  res = pdp.takeDecision()
##  print res
##
##print "\n\n ~~~~~~~ SERVICE 2 ~~~~~~~ : %s \n " %servizio2
##
##for status in ValidStatus:
###  for oldStatus in ValidStatus:
###    if status == oldStatus:
###      continue
##    print "############################"
##    print " "
##    print 'nel test:', status#, oldStatus
##    pdp = PDP(VO, granularity = 'Service', name = servizio2['name'], status = status,
###              formerStatus = oldStatus,
##              reason = 'XXXXX', siteType = servizio2['siteType'],
##              serviceType = servizio2['serviceType'],
##              useNewRes = useNewRes
##              )
##    res = pdp.takeDecision()
##    print res
##
##print "\n\n ~~~~~~~ SERVICE 3 ~~~~~~~ : %s \n " %servizio3
##
##for status in ValidStatus:
###  for oldStatus in ValidStatus:
###    if status == oldStatus:
###      continue
##    print "############################"
##    print " "
##    print 'nel test:', status#, oldStatus
##    pdp = PDP(VO, granularity = 'Service', name = servizio3['name'], status = status,
###              formerStatus = oldStatus,
##              reason = 'XXXXX', siteType = servizio3['siteType'],
##              serviceType = servizio3['serviceType'],
##              useNewRes = useNewRes
##              )
##    res = pdp.takeDecision()
##    print res
##
##print "\n\n ~~~~~~~ SERVICE 4 ~~~~~~~ : %s \n " %servizio4
##
##for status in ValidStatus:
###  for oldStatus in ValidStatus:
###    if status == oldStatus:
###      continue
##    print "############################"
##    print " "
##    print 'nel test:', status#, oldStatus
##    pdp = PDP(VO, granularity = 'Service', name = servizio4['name'], status = status,
###              formerStatus = oldStatus,
##              reason = 'XXXXX', siteType = servizio4['siteType'],
##              serviceType = servizio4['serviceType'],
##              useNewRes = useNewRes
##              )
##    res = pdp.takeDecision()
##    print res
##
##
##
#print "\n\n ~~~~~~~ RISORSA 1 ~~~~~~~ : %s \n " %risorsa
#
#for status in ValidStatus:
##  for oldStatus in ValidStatus:
##    if status == oldStatus:
##      continue
#  print "############################"
#  print " "
#  print status#, oldStatus
#  pdp = PDP(VO, granularity = 'Resource', name = risorsa['name'], status = status,
##            formerStatus = oldStatus,
#            reason = 'XXXXX', siteType = risorsa['siteType'],
#            resourceType = risorsa['resourceType'],
#            useNewRes = useNewRes
#            )
#  res = pdp.takeDecision()
#  print res
#
##print "\n\n ~~~~~~~ RISORSA 2 ~~~~~~~ : %s \n " %risorsa2
##
##for status in ValidStatus:
###  for oldStatus in ValidStatus:
###    if status == oldStatus:
###      continue
##  print "############################"
##  print " "
##  print status#, oldStatus
##  pdp = PDP(VO, granularity = 'Resource', name = risorsa2['name'], status = status,
###              formerStatus = oldStatus,
##            reason = 'XXXXX', siteType = risorsa2['siteType'],
##            resourceType = risorsa2['resourceType'],
##            useNewRes = useNewRes
##            )
##  res = pdp.takeDecision()
##  print res
##
##print "\n\n ~~~~~~~ RISORSA 3 ~~~~~~~ : %s \n " %risorsa3
##
##for status in ValidStatus:
###  for oldStatus in ValidStatus:
###    if status == oldStatus:
###      continue
##  print "############################"
##  print " "
##  print status#, oldStatus
##  pdp = PDP(VO, granularity = 'Resource', name = risorsa3['name'], status = status,
###            formerStatus = oldStatus,
##            reason = 'XXXXX', siteType = risorsa3['siteType'],
##            resourceType = risorsa3['resourceType'],
##            useNewRes = useNewRes
##            )
##  res = pdp.takeDecision()
##  print res
##
##print "\n\n ~~~~~~~ RISORSA 4 ~~~~~~~ : %s \n " %risorsa4
##
##for status in ValidStatus:
###  for oldStatus in ValidStatus:
###    if status == oldStatus:
###      continue
##  print "############################"
##  print " "
##  print status#, oldStatus
##  pdp = PDP(VO, granularity = 'Resource', name = risorsa4['name'], status = status,
###            formerStatus = oldStatus,
##            reason = 'XXXXX', siteType = risorsa3['siteType'],
##            resourceType = risorsa4['resourceType'],
##            useNewRes = useNewRes
##            )
##  res = pdp.takeDecision()
##  print res
##
##print "\n\n ~~~~~~~ RISORSA 5 ~~~~~~~ : %s \n " %risorsa5
##
##for status in ValidStatus:
###  for oldStatus in ValidStatus:
###    if status == oldStatus:
###      continue
##  print "############################"
##  print " "
##  print status#, oldStatus
##  pdp = PDP(VO, granularity = 'Resource', name = risorsa5['name'], status = status,
###            formerStatus = oldStatus,
##            reason = 'XXXXX', siteType = risorsa5['siteType'],
##            resourceType = risorsa5['resourceType'],
##            useNewRes = useNewRes
##            )
##  res = pdp.takeDecision()
##  print res
##
##
##print "\n\n ~~~~~~~ StorageElement ~~~~~~~ : %s \n " %se
##
##for status in ValidStatus:
###  for oldStatus in ValidStatus:
###    if status == oldStatus:
###      continue
##  print "############################"
##  print " "
##  print status#, oldStatus
##  pdp = PDP(VO, granularity = 'StorageElement', name = se['name'], status = status,
###            formerStatus = oldStatus,
##            reason = 'XXXXX', siteType = se['siteType'],
##            useNewRes = useNewRes
##            )
##  res = pdp.takeDecision()
##  print res
#
#################################################################################
## # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
#################################################################################
#
#'''
#  HOW DOES THIS WORK.
#    
#    will come soon...
#'''
#            
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF