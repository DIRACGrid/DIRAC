""" This is a test of the chain
    ProductionClient -> ProductionManagerHandler -> ProductionDB

    It supposes that the ProductionDB, TransformationDB and the FileCatalogDB to be present
    It supposes the ProductionManager, TransformationManager and that DataManagement/FileCatalog services running
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest
import json

from DIRAC.ProductionSystem.Client.ProductionClient   import ProductionClient
from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

class TestClientProductionTestCase( unittest.TestCase ):

  def setUp( self ):
    self.prodClient = ProductionClient()
    self.transClient = TransformationClient()
    self.fc = FileCatalog()

    # ## Add metadata fields to the DFC
    self.MDFieldDict = {'particle':'VARCHAR(128)', 'analysis_prog':'VARCHAR(128)', 'tel_sim_prog':'VARCHAR(128)', 'outputType':'VARCHAR(128)', 'zenith':'int', 'data_level': 'int' }
    for MDField in self.MDFieldDict:
      MDFieldType = self.MDFieldDict[MDField]
      res = self.fc.addMetadataField( MDField, MDFieldType )
      self.assert_( res['OK'] )

  def tearDown( self ):
    ### Delete meta data fields
    for MDField in self.MDFieldDict:
      res = self.fc.deleteMetadataField( MDField )
      self.assert_( res['OK'] )

class ProductionClientChain( TestClientProductionTestCase ):

  def test_SeqProduction(self):

    ### Define the production

    ### Define the first step of the production
    prodStep1 = {}
    prodStep1['type'] = 'MCSimulation'
    outputquery = {'zenith':{'in': [20, 40]},'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':{'in': ['Data', 'Log']}}
    prodStep1['outputquery'] = outputquery

    ### Define the second step of the production
    prodStep2 = {}
    prodStep2['type'] = 'DataProcessing'
    prodStep2['parentStep'] = prodStep1

    inputquery = {'zenith': 20, 'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':'Data'}
    outputquery = {'zenith': 20, 'particle':'gamma', 'analysis_prog': 'evndisp', 'data_level': 1, 'outputType':{'in': ['Data', 'Log']}}

    prodStep2['inputquery'] = inputquery
    prodStep2['outputquery'] = outputquery

    ### Define the third step of the production
    prodStep3 = {}
    prodStep3['type'] = 'DataProcessing'
    prodStep3['parentStep'] = prodStep2

    inputquery = {'zenith': 20, 'particle':'gamma', 'analysis_prog':'evndisp', 'data_level': 1, 'outputType':'Data'}
    outputquery = {'zenith': 20, 'particle':'gamma', 'analysis_prog': 'evndisp', 'data_level': 2, 'outputType':{'in': ['Data', 'Log']}}

    prodStep3['inputquery'] = inputquery
    prodStep3['outputquery'] = outputquery

    ## Add the steps to the prod description
    self.prodClient.addStep(prodStep1)
    self.prodClient.addStep(prodStep2)
    self.prodClient.addStep(prodStep3)

    ## Get the production description
    prodDescription = self.prodClient.getDescription()

    ## Create the production
    prodName = 'SeqProd'
    res = self.prodClient.createProduction( prodName, json.dumps(prodDescription) )
    self.assertTrue( res['OK'] )

    ## Start the production, i.e. instatiate the transformation steps
    res = self.prodClient.startProduction( prodName )
    self.assertTrue( res['OK'] )

    ### Get the transformations of the production
    res = self.prodClient.getProduction( prodName )
    self.assertTrue( res['OK'] )
    prodID = res['Value']['ProductionID']

    res = self.prodClient.getProductionTransformations( prodID )
    self.assertTrue( res['OK'] )
    self.assertEqual( len( res['Value'] ) , 3 )

    ### Delete the production
    res = self.prodClient.deleteProduction( prodName)
    self.assertTrue( res['OK'] )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestClientProductionTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionClientChain ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
