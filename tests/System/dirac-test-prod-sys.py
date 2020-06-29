""" This script submits a test production
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=wrong-import-position, protected-access

import os
import json

from DIRAC.Core.Base import Script

Script.parseCommandLine()

# from DIRAC
from DIRAC import gLogger
from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient
from DIRAC.ProductionSystem.Client.ProductionStep import ProductionStep
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog


def createWorkflowBodyStep1():
  job = Job()
  job.setName('mandelbrot raw')
  job.setOutputSandbox(['*log'])
  # this is so that the JOB_ID within the transformation can be evaluated on the fly in the job application, see below
  job.workflow.addParameter(Parameter("JOB_ID", "000000", "string", "", "", True, False, "Initialize JOB_ID"))
  # define the job workflow in 3 steps
  # job step1: setup software
  job.setExecutable('git clone https://github.com/bregeon/mandel4ts.git')
  # job step2: run mandelbrot application
  # note how the JOB_ID (within the transformation) is passed as an argument and will be evaluated on the fly
  job.setExecutable('./mandel4ts/mandelbrot.py', arguments="-P 0.0005 -M 1000 -L @{JOB_ID} -N 200")
  # job step3: upload data and set metadata
  outputPath = os.path.join('/dirac/prodsys/mandelbrot/images/raw')
  outputPattern = 'data_*txt'
  outputSE = 'RAL-SE'
  outputMetadata = json.dumps({"application": "mandelbrot", "image_format": "ascii",
                               "image_width": 7680, "image_height": 200})
  job.setExecutable('./mandel4ts/dirac-add-files.py', arguments="%s '%s' %s '%s'" %
                    (outputPath, outputPattern, outputSE, outputMetadata))
  return job.workflow.toXML()


def createWorkflowBodyStep2():
  job = Job()
  job.setName('merge mandelbrot')
  job.setOutputSandbox(['*log'])

  # define the job workflow in 3 steps
  # job step1: setup software
  job.setExecutable('git clone https://github.com/bregeon/mandel4ts.git')
  # job step2: run mandelbrot merge
  job.setExecutable('./mandel4ts/merge_data.py')
  # job step3: upload data and set metadata
  outputPath = os.path.join('/dirac/prodsys/mandelbrot/images/merged')
  outputPattern = 'data_merged*txt'
  outputSE = 'RAL-SE'
  nb_input_files = 7
  outputMetadata = json.dumps({"application": "mandelbrot", "image_format": "ascii",
                               "image_width": 7680, "image_height": 200 * nb_input_files})
  job.setExecutable('./mandel4ts/dirac-add-files.py', arguments="%s '%s' %s '%s'" %
                    (outputPath, outputPattern, outputSE, outputMetadata))
  return job.workflow.toXML()


def createProductionStep(name, type, inputQuery=None, outputQuery=None):
  # create a production step
  prodStep = ProductionStep()
  prodStep.Name = name
  prodStep.Type = type
  prodStep.Inputquery = inputQuery
  prodStep.Outputquery = outputQuery
  return prodStep


# Set meta data fields in the DFC
fc = FileCatalog()
MDFieldDict = {
    'application': 'VARCHAR(128)',
    'image_format': 'VARCHAR(128)',
    'image_width': 'int',
    'image_height': 'int'}
for MDField in MDFieldDict.keys():
  MDFieldType = MDFieldDict[MDField]
  res = fc.addMetadataField(MDField, MDFieldType)
  if not res['OK']:
    gLogger.error("Failed to add metadata fields", res['Message'])
    exit(-1)

# Instantiate the ProductionClient
prodClient = ProductionClient()

# Create the first production step and add it to the Production
outputquery = {"application": "mandelbrot", "image_format": "ascii", "image_width": 7680, "image_height": 200}
prodStep1 = createProductionStep('ImageProd', 'MCSimulation', outputQuery=outputquery)
body = createWorkflowBodyStep1()
prodStep1.Body = body
res = prodClient.addProductionStep(prodStep1)
if not res['OK']:
  gLogger.error("Failed to add production step", res['Message'])
  exit(-1)

# Create the second production step and add it to the Production
inputquery = {"application": "mandelbrot", "image_format": "ascii", "image_width": 7680, "image_height": 200}
outputquery = {"application": "mandelbrot", "image_format": "ascii", "image_width": 7680, "image_height": 1400}
prodStep2 = createProductionStep('MergeImage', 'DataProcessing', inputQuery=inputquery, outputQuery=outputquery)

body = createWorkflowBodyStep2()
prodStep2.Body = body
prodStep2.ParentStep = prodStep1
res = prodClient.addProductionStep(prodStep2)
if not res['OK']:
  gLogger.error("Failed to add production step", res['Message'])
  exit(-1)

# Get the production description
prodDesc = prodClient.prodDescription

# Create the production
prodName = 'SeqProd'
res = prodClient.addProduction(prodName, json.dumps(prodDesc))
if not res['OK']:
  gLogger.error("Failed to add production", res['Message'])
  exit(-1)

# Start the production, i.e. instantiate the transformation steps
res = prodClient.startProduction(prodName)
if not res['OK']:
  gLogger.error("Failed to start production", res['Message'])
  exit(-1)
