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
from DIRAC.DataManagementSystem.Client.MetaQuery import MetaQuery

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

  def test_addTransformationsToProduction(self):
    ### Create a production
    res = self.prodClient.addProduction( 'FullProd' )
    self.assertTrue( res['OK'] )
    prodID = res['Value']

    ## Create a transformation and set the status to Active
    res = self.transClient.addTransformation( 'GenTrans', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '' )
    self.assertTrue( res['OK'] )
    transID = res['Value']

    res = self.transClient.setTransformationParameter( transID, 'Status', 'Active' )
    self.assertTrue( res['OK'] )

    ### Add to the production the transformation without any parent trans (-1)
    res = self.prodClient.addTransformationsToProduction( prodID, transID, -1 )
    self.assertFalse( res['OK'] )


    ### Create a MCSimulation transformation with an output meta query and output metadata
    outputquery = MetaQuery( {'zenith':{'in': [20, 40]},'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':{'in': ['Data', 'Log']}} )
    outputquery = outputquery.getMetaQueryAsJson()
    ### Not used in the Production validation for the moment
    outputmetadata = {'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':'Data'}

    res = self.transClient.addTransformation( 'MCSim', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '', outputMetaQuery=outputquery, outputMetaData=json.dumps(outputmetadata) )

    self.assertTrue( res['OK'] )
    MCtransID = res['Value']

    ### Create a DataProcessing transformation with an input and output meta query
    inputquery = MetaQuery( {'zenith': 20, 'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':'Data'} )
    outputquery = MetaQuery( {'particle':'gamma', 'analysis_prog':{'in': ['evndisp', 'mars']}, 'outputType':{'in': ['Data', 'Log']}} )

    inputquery = inputquery.getMetaQueryAsJson()
    outputquery = outputquery.getMetaQueryAsJson()

    res = self.transClient.addTransformation( 'Analysis1', 'description', 'longDescription', 'DataProcessing', 'Standard',
                                              'Manual', '', inputMetaQuery=inputquery, outputMetaQuery=outputquery )

    self.assertTrue( res['OK'] )
    transID = res['Value']

    ### Add to the production the MCSim transformation without any parent trans (-1)
    res = self.prodClient.addTransformationsToProduction( prodID, MCtransID, -1 )
    self.assertTrue( res['OK'] )

    ## 1. Valid Production case
    ### Add to the production the DataProcessing transformation with the MCSim transformation as parent trans
    res = self.prodClient.addTransformationsToProduction( prodID, transID, MCtransID )
    self.assertTrue( res['OK'] )

    ### 1. Invalid Production case: not maching Metadata values
    inputquery = MetaQuery( {'zenith': 20, 'particle':'proton', 'tel_sim_prog':'simtel', 'outputType':'Data'} )
    inputquery = inputquery.getMetaQueryAsJson()

    res = self.transClient.addTransformation( 'Analysis2', 'description', 'longDescription', 'DataProcessing', 'Standard',
                                              'Manual', '', inputMetaQuery=inputquery, outputMetaQuery=outputquery )

    self.assertTrue( res['OK'] )
    transID = res['Value']

    res = self.prodClient.addTransformationsToProduction( prodID, transID, MCtransID )
    self.assertFalse( res['OK'] )

    ### 2. Invalid Production case: not supported Pperation
    inputquery = MetaQuery( {'zenith': {'>=': 20}, 'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':'Data'} )
    inputquery = inputquery.getMetaQueryAsJson()

    res = self.transClient.addTransformation( 'Analysis3', 'description', 'longDescription', 'DataProcessing', 'Standard',
                                              'Manual', '', inputMetaQuery=inputquery, outputMetaQuery=outputquery )

    self.assertTrue( res['OK'] )
    transID = res['Value']

    res = self.prodClient.addTransformationsToProduction( prodID, transID, MCtransID )
    self.assertFalse( res['OK'] )

    ### 3. Invalid Production case: not supported Metatype. Only int, float, string are supported

    ### Change the type of Add metadata fields to the DFC
    self.fc.deleteMetadataField( 'zenith' )
    self.fc.addMetadataField( 'zenith', 'double' )

    inputquery = MetaQuery( {'zenith': 20, 'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':'Data'} )
    inputquery = inputquery.getMetaQueryAsJson()

    res = self.transClient.addTransformation( 'Analysis4', 'description', 'longDescription', 'DataProcessing', 'Standard',
                                              'Manual', '', inputMetaQuery=inputquery, outputMetaQuery=outputquery )

    self.assertTrue( res['OK'] )
    transID = res['Value']

    res = self.prodClient.addTransformationsToProduction( prodID, transID, MCtransID )
    self.assertFalse( res['OK'] )


    ### Delete the transformations that are not associated to the production
    for transName in ['GenTrans', 'Analysis2', 'Analysis3','Analysis4']:
      res = self.transClient.deleteTransformation( transName )
      self.assertTrue( res['OK'] )

    ### Delete the production. The associated transformations are also deleted
    res = self.prodClient.deleteProduction( 'FullProd' )
    self.assertTrue( res['OK'] )

    ### Reset the zenith metadata to the original type in the DFC
    self.fc.deleteMetadataField( 'zenith' )
    self.fc.addMetadataField( 'zenith', 'int' )

  def test_SeqProduction(self):

    ### Create a production
    prodName = 'SeqProd'
    res = self.prodClient.addProduction( prodName )
    self.assertTrue( res['OK'] )
    prodID = res['Value']

    ### Create a MCSimulation transformation with an output meta query and output metadata
    outputquery = MetaQuery( {'zenith':{'in': [20, 40]},'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':{'in': ['Data', 'Log']}} )
    outputquery = outputquery.getMetaQueryAsJson()
    ### Not used in the Production validation for the moment
    outputmetadata = {'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':'Data'}

    res = self.transClient.addTransformation( 'MCSim', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '', outputMetaQuery=outputquery, outputMetaData=json.dumps(outputmetadata) )

    self.assertTrue( res['OK'] )
    MCtransID = res['Value']

    ### Create the Analysis_step1 transformation
    inputquery = MetaQuery( {'zenith': 20, 'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':'Data'} )
    outputquery = MetaQuery( {'zenith': 20, 'particle':'gamma', 'analysis_prog': 'evndisp', 'data_level': 1, 'outputType':{'in': ['Data', 'Log']}} )

    inputquery = inputquery.getMetaQueryAsJson()
    outputquery = outputquery.getMetaQueryAsJson()

    res = self.transClient.addTransformation( 'Analysis_step1', 'description', 'longDescription', 'DataProcessing', 'Standard',
                                              'Manual', '', inputMetaQuery=inputquery, outputMetaQuery=outputquery )

    self.assertTrue( res['OK'] )
    analysis_step1_transID = res['Value']

    ### Create the Analysis_step2 transformation
    inputquery = MetaQuery( {'zenith': 20, 'particle':'gamma', 'analysis_prog':'evndisp', 'data_level': 1, 'outputType':'Data'} )
    outputquery = MetaQuery( {'zenith': 20, 'particle':'gamma', 'analysis_prog': 'evndisp', 'data_level': 2, 'outputType':{'in': ['Data', 'Log']}} )

    inputquery = inputquery.getMetaQueryAsJson()
    outputquery = outputquery.getMetaQueryAsJson()

    res = self.transClient.addTransformation( 'Analysis_step2', 'description', 'longDescription', 'DataProcessing', 'Standard',
                                              'Manual', '', inputMetaQuery=inputquery, outputMetaQuery=outputquery )

    self.assertTrue( res['OK'] )
    analysis_step2_transID = res['Value']

    ### Add to the production the MCSim transformation
    res = self.prodClient.addTransformationsToProduction( prodID, MCtransID, -1 )
    self.assertTrue( res['OK'] )

    ### Add to the production Analysis_step1 transformation with the MCSim transformation as parent trans
    res = self.prodClient.addTransformationsToProduction( prodID, analysis_step1_transID, MCtransID )
    self.assertTrue( res['OK'] )

    ### Add to the production Analysis_step2 transformation with the Analysis_step1 transformation as parent trans
    res = self.prodClient.addTransformationsToProduction( prodID, analysis_step2_transID, analysis_step1_transID )
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

  def test_HatProduction(self):

    ### Create a production
    prodName = 'HatProd'
    res = self.prodClient.addProduction( prodName )
    self.assertTrue( res['OK'] )
    prodID = res['Value']

    # ## Add metadata fields to the DFC
    MDFieldDict = {'particle':'VARCHAR(128)', 'analysis_prog':'VARCHAR(128)', 'tel_sim_prog':'VARCHAR(128)', 'outputType':'VARCHAR(128)', 'zenith':'int', 'data_level': 'int' }
    for MDField in MDFieldDict.keys():
      MDFieldType = MDFieldDict[MDField]
      res = self.fc.addMetadataField( MDField, MDFieldType )
      self.assert_( res['OK'] )

    ### Create a MCSimulation transformation with an output meta query and output metadata
    outputquery = MetaQuery( {'zenith':{'in': [20, 40]},'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':{'in': ['Data', 'Log']}} )
    outputquery = outputquery.getMetaQueryAsJson()
    ### Not used in the Production validation for the moment
    outputmetadata = {'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':'Data'}

    res = self.transClient.addTransformation( 'MCSim', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '', outputMetaQuery=outputquery, outputMetaData=json.dumps(outputmetadata) )

    self.assertTrue( res['OK'] )
    MCtransID = res['Value']

    ### Create the Analysis_step1 transformation
    inputquery = MetaQuery( {'zenith': 20, 'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':'Data'} )
    outputquery = MetaQuery( {'zenith': 20, 'particle':'gamma', 'analysis_prog': 'evndisp', 'data_level': 1, 'outputType':{'in': ['Data', 'Log']}} )

    inputquery = inputquery.getMetaQueryAsJson()
    outputquery = outputquery.getMetaQueryAsJson()

    res = self.transClient.addTransformation( 'Analysis_step1a', 'description', 'longDescription', 'DataProcessing', 'Standard',
                                              'Manual', '', inputMetaQuery=inputquery, outputMetaQuery=outputquery )

    self.assertTrue( res['OK'] )
    analysis_step1a_transID = res['Value']

    ### Create the Analysis_step1b transformation
    inputquery = MetaQuery( {'zenith': 40, 'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':'Data'} )
    outputquery = MetaQuery( {'zenith': 40, 'particle':'gamma', 'analysis_prog': 'mars', 'data_level': 1, 'outputType':{'in': ['Data', 'Log']}} )

    inputquery = inputquery.getMetaQueryAsJson()
    outputquery = outputquery.getMetaQueryAsJson()

    res = self.transClient.addTransformation( 'Analysis_step1b', 'description', 'longDescription', 'DataProcessing', 'Standard',
                                              'Manual', '', inputMetaQuery=inputquery, outputMetaQuery=outputquery )

    self.assertTrue( res['OK'] )
    analysis_step1b_transID = res['Value']

    ### Add to the production the MCSim transformation
    res = self.prodClient.addTransformationsToProduction( prodID, MCtransID, -1 )
    self.assertTrue( res['OK'] )

    ### Add to the production Analysis_step1a and Analysis_step1b transformations with MCSim as parent trans
    res = self.prodClient.addTransformationsToProduction( prodID, [analysis_step1a_transID, analysis_step1b_transID], MCtransID )
    self.assertTrue( res['OK'] )

    ### Get the transformations of the production
    res = self.prodClient.getProduction( prodName )
    self.assertTrue( res['OK'] )
    prodID = res['Value']['ProductionID']

    res = self.prodClient.getProductionTransformations( prodID )
    self.assertTrue( res['OK'] )
    self.assertEqual( len( res['Value'] ) , 3 )

    ### Delete the production
    res = self.prodClient.deleteProduction( prodName )
    self.assertTrue( res['OK'] )

  def test_VProduction(self):

    ### Create a production
    prodName = 'VProd'
    res = self.prodClient.addProduction( prodName )
    self.assertTrue( res['OK'] )
    prodID = res['Value']

    ### Create a MCSimulation transformation with an output meta query and output metadata
    outputquery = MetaQuery( {'zenith':20, 'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':{'in': ['Data', 'Log']}} )
    outputquery = outputquery.getMetaQueryAsJson()
    ### Not used in the Production validation for the moment
    outputmetadata = {'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':'Data'}

    res = self.transClient.addTransformation( 'MCSimA', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '', outputMetaQuery=outputquery, outputMetaData=json.dumps(outputmetadata) )

    self.assertTrue( res['OK'] )
    MCSimAtransID = res['Value']

    ### Create a second MCSimulation transformation with an output meta query and output metadata
    outputquery = MetaQuery( {'zenith':40, 'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':{'in': ['Data', 'Log']}} )
    outputquery = outputquery.getMetaQueryAsJson()
    ### Not used in the Production validation for the moment
    outputmetadata = {'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':'Data'}

    res = self.transClient.addTransformation( 'MCSimB', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '', outputMetaQuery=outputquery, outputMetaData=json.dumps(outputmetadata) )

    self.assertTrue( res['OK'] )
    MCSimBtransID = res['Value']

    ### Create the Analysis_step1 transformation
    inputquery = MetaQuery( {'zenith': {'in': [20, 40]}, 'particle':'gamma', 'tel_sim_prog':'simtel', 'outputType':'Data'} )
    outputquery = MetaQuery( {'zenith': {'in': [20, 40]}, 'particle':'gamma', 'analysis_prog': 'evndisp', 'data_level': 1, 'outputType':{'in': ['Data', 'Log']}} )

    inputquery = inputquery.getMetaQueryAsJson()
    outputquery = outputquery.getMetaQueryAsJson()

    res = self.transClient.addTransformation( 'Analysis_step1', 'description', 'longDescription', 'DataProcessing', 'Standard',
                                              'Manual', '', inputMetaQuery=inputquery, outputMetaQuery=outputquery )

    self.assertTrue( res['OK'] )
    analysis_step1_transID = res['Value']

    ### Add to the production the MCSim transformations
    res = self.prodClient.addTransformationsToProduction( prodID, [MCSimAtransID, MCSimBtransID], -1 )
    self.assertTrue( res['OK'] )

    ### Add to the production Analysis_step1 transformation with MCaSim and MCbSim as parent trans
    res = self.prodClient.addTransformationsToProduction( prodID, analysis_step1_transID, [MCSimAtransID, MCSimBtransID] )
    self.assertTrue( res['OK'] )

    ### Get the transformations of the production
    res = self.prodClient.getProduction( prodName )
    self.assertTrue( res['OK'] )
    prodID = res['Value']['ProductionID']

    res = self.prodClient.getProductionTransformations( prodID )
    self.assertTrue( res['OK'] )
    self.assertEqual( len( res['Value'] ) , 3 )

    ### Delete the production
    res = self.prodClient.deleteProduction( prodName )
    self.assertTrue( res['OK'] )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestClientProductionTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionClientChain ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
