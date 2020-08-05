""" This is a test of the chain
    ProductionClient -> ProductionManagerHandler -> ProductionDB

    It supposes that the ProductionDB, TransformationDB and the FileCatalogDB to be present
    It supposes the ProductionManager, TransformationManager and that DataManagement/FileCatalog services running
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest
import json

from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient
from DIRAC.ProductionSystem.Client.ProductionStep import ProductionStep
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog


class TestClientProductionTestCase(unittest.TestCase):

  def setUp(self):
    self.prodClient = ProductionClient()
    self.transClient = TransformationClient()
    self.fc = FileCatalog()

    # ## Add metadata fields to the DFC
    self.MDFieldDict = {
        'particle': 'VARCHAR(128)',
        'analysis_prog': 'VARCHAR(128)',
        'tel_sim_prog': 'VARCHAR(128)',
        'outputType': 'VARCHAR(128)',
        'zenith': 'int',
        'data_level': 'int'}
    for MDField in self.MDFieldDict:
      MDFieldType = self.MDFieldDict[MDField]
      res = self.fc.addMetadataField(MDField, MDFieldType)
      self.assert_(res['OK'])

  def tearDown(self):
    # Delete meta data fields
    for MDField in self.MDFieldDict:
      res = self.fc.deleteMetadataField(MDField)
      self.assert_(res['OK'])


class ProductionClientChain(TestClientProductionTestCase):

  def test_SeqProduction(self):

    # Define the first step of the production
    prodStep1 = ProductionStep()
    prodStep1.Name = 'Sim_prog'
    prodStep1.Type = 'MCSimulation'
    outputquery = {
        'zenith': {
            'in': [
                20,
                40]},
        'particle': 'gamma',
        'tel_sim_prog': 'simtel',
        'outputType': {
            'in': [
                'Data',
                'Log']}}
    prodStep1.Outputquery = outputquery

    # Add the step to the production
    res = self.prodClient.addProductionStep(prodStep1)
    self.assertTrue(res['OK'])

    # Define the second step of the production
    prodStep2 = ProductionStep()
    prodStep2.Name = 'Reco_prog'
    prodStep2.Type = 'DataProcessing'
    prodStep2.ParentStep = prodStep1

    inputquery = {'zenith': 20, 'particle': 'gamma', 'tel_sim_prog': 'simtel', 'outputType': 'Data'}
    outputquery = {
        'zenith': 20,
        'particle': 'gamma',
        'analysis_prog': 'evndisp',
        'data_level': 1,
        'outputType': {
            'in': [
                'Data',
                'Log']}}

    prodStep2.Inputquery = inputquery
    prodStep2.Outputquery = outputquery

    # Add the step to the production
    res = self.prodClient.addProductionStep(prodStep2)
    self.assertTrue(res['OK'])

    # Define the third step of the production
    prodStep3 = ProductionStep()
    prodStep3.Name = 'Analyis_prog'
    prodStep3.Type = 'DataProcessing'
    prodStep3.ParentStep = prodStep2

    inputquery = {'zenith': 20, 'particle': 'gamma', 'analysis_prog': 'evndisp', 'data_level': 1, 'outputType': 'Data'}
    outputquery = {
        'zenith': 20,
        'particle': 'gamma',
        'analysis_prog': 'evndisp',
        'data_level': 2,
        'outputType': {
            'in': [
                'Data',
                'Log']}}

    prodStep3.Inputquery = inputquery
    prodStep3.Outputquery = outputquery

    # Add the step to the production
    res = self.prodClient.addProductionStep(prodStep3)
    self.assertTrue(res['OK'])

    # Get the production description
    prodDesc = self.prodClient.prodDescription

    # Create the production
    prodName = 'SeqProd'
    res = self.prodClient.addProduction(prodName, json.dumps(prodDesc))
    self.assertTrue(res['OK'])

    # Start the production, i.e. instatiate the transformation steps
    res = self.prodClient.startProduction(prodName)
    self.assertTrue(res['OK'])

    # Get the transformations of the production
    res = self.prodClient.getProduction(prodName)
    self.assertTrue(res['OK'])
    prodID = res['Value']['ProductionID']

    res = self.prodClient.getProductionTransformations(prodID)
    self.assertTrue(res['OK'])
    self.assertEqual(len(res['Value']), 3)

    # Delete the production
    res = self.prodClient.deleteProduction(prodName)
    self.assertTrue(res['OK'])

  def test_MergeProduction(self):

    # Define the first step of the production
    prodStep1 = ProductionStep()
    prodStep1.Name = 'Sim_prog'
    prodStep1.Type = 'MCSimulation'
    outputquery = {'zenith': 20, 'particle': 'gamma', 'tel_sim_prog': 'simtel', 'outputType': {'in': ['Data', 'Log']}}
    prodStep1.Outputquery = outputquery

    # Add the step to the production
    res = self.prodClient.addProductionStep(prodStep1)
    self.assertTrue(res['OK'])

    # Define the second step of the production
    prodStep2 = ProductionStep()
    prodStep2.Name = 'Sim_prog'
    prodStep2.Type = 'MCSimulation'
    outputquery = {'zenith': 40, 'particle': 'gamma', 'tel_sim_prog': 'simtel', 'outputType': {'in': ['Data', 'Log']}}
    prodStep2.Outputquery = outputquery

    # Add the step to the production
    res = self.prodClient.addProductionStep(prodStep2)
    self.assertTrue(res['OK'])

    # Define the third step of the production
    prodStep3 = ProductionStep()
    prodStep3.Name = 'Reco_prog'
    prodStep3.Type = 'DataProcessing'
    prodStep3.ParentStep = [prodStep1, prodStep2]

    inputquery = {'zenith': {'in': [20, 40]}, 'particle': 'gamma', 'tel_sim_prog': 'simtel', 'outputType': 'Data'}
    outputquery = {
        'zenith': {
            'in': [
                20,
                40]},
        'particle': 'gamma',
        'analysis_prog': 'evndisp',
        'data_level': 1,
        'outputType': {
            'in': [
                'Data',
                'Log']}}

    prodStep3.Inputquery = inputquery
    prodStep3.Outputquery = outputquery

    # Add the steps to the production
    res = self.prodClient.addProductionStep(prodStep3)
    self.assertTrue(res['OK'])

    # Get the production description
    prodDesc = self.prodClient.prodDescription

    # Create the production
    prodName = 'MergeProd'
    res = self.prodClient.addProduction(prodName, json.dumps(prodDesc))
    self.assertTrue(res['OK'])

    # Start the production, i.e. instatiate the transformation steps
    res = self.prodClient.startProduction(prodName)
    self.assertTrue(res['OK'])

    # Get the transformations of the production
    res = self.prodClient.getProduction(prodName)
    self.assertTrue(res['OK'])
    prodID = res['Value']['ProductionID']

    res = self.prodClient.getProductionTransformations(prodID)
    self.assertTrue(res['OK'])
    self.assertEqual(len(res['Value']), 3)

    # Delete the production
    res = self.prodClient.deleteProduction(prodName)
    self.assertTrue(res['OK'])

  def test_SplitProduction(self):

    # Define the first step of the production
    prodStep1 = ProductionStep()
    prodStep1.Name = 'Sim_prog'
    prodStep1.Type = 'MCSimulation'
    outputquery = {
        'zenith': {
            'in': [
                20,
                40]},
        'particle': 'gamma',
        'tel_sim_prog': 'simtel',
        'outputType': {
            'in': [
                'Data',
                'Log']}}
    prodStep1.Outputquery = outputquery

    # Add the step to the production
    res = self.prodClient.addProductionStep(prodStep1)
    self.assertTrue(res['OK'])

    # Define the second step of the production
    prodStep2 = ProductionStep()
    prodStep2.Name = 'Reco_prog'
    prodStep2.Type = 'DataProcessing'
    prodStep2.ParentStep = prodStep1

    inputquery = {'zenith': 20, 'particle': 'gamma', 'tel_sim_prog': 'simtel', 'outputType': 'Data'}
    outputquery = {
        'zenith': 20,
        'particle': 'gamma',
        'analysis_prog': 'evndisp',
        'data_level': 1,
        'outputType': {
            'in': [
                'Data',
                'Log']}}
    prodStep2.Inputquery = inputquery
    prodStep2.Outputquery = outputquery

    # Add the step to the production
    res = self.prodClient.addProductionStep(prodStep2)
    self.assertTrue(res['OK'])

    # Define the third step of the production
    prodStep3 = ProductionStep()
    prodStep3.Name = 'Reco_prog'
    prodStep3.Type = 'DataProcessing'
    prodStep3.ParentStep = prodStep1

    inputquery = {'zenith': 40, 'particle': 'gamma', 'tel_sim_prog': 'simtel', 'outputType': 'Data'}
    outputquery = {
        'zenith': 40,
        'particle': 'gamma',
        'analysis_prog': 'evndisp',
        'data_level': 1,
        'outputType': {
            'in': [
                'Data',
                'Log']}}

    prodStep3.Inputquery = inputquery
    prodStep3.Outputquery = outputquery

    # Add the steps to the production
    res = self.prodClient.addProductionStep(prodStep3)
    self.assertTrue(res['OK'])

    # Get the production description
    prodDesc = self.prodClient.prodDescription

    # Create the production
    prodName = 'SplitProd'
    res = self.prodClient.addProduction(prodName, json.dumps(prodDesc))
    self.assertTrue(res['OK'])

    # Start the production, i.e. instatiate the transformation steps
    res = self.prodClient.startProduction(prodName)
    self.assertTrue(res['OK'])

    # Get the transformations of the production
    res = self.prodClient.getProduction(prodName)
    self.assertTrue(res['OK'])
    prodID = res['Value']['ProductionID']

    res = self.prodClient.getProductionTransformations(prodID)
    self.assertTrue(res['OK'])
    self.assertEqual(len(res['Value']), 3)

    # Delete the production
    res = self.prodClient.deleteProduction(prodName)
    self.assertTrue(res['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestClientProductionTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ProductionClientChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
