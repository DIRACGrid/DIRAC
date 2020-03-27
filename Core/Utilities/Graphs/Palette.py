""" Palette is a tool to generate colors for various Graphs plots and legends

    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""

__RCSID__ = "$Id$"

import hashlib

from DIRAC.WorkloadManagementSystem.Client import JobStatus

job_status_palette = {
    JobStatus.RECEIVED: '#D9E7F8',
    JobStatus.CHECKING: '#FAFAFA',
    JobStatus.STAGING: '#6190CD',
    JobStatus.WAITING: '#004EFF',
    JobStatus.MATCHED: '#FEF7AA',
    JobStatus.RUNNING: '#FDEE65',
    JobStatus.COMPLETING: '#FFAF55',
    JobStatus.STALLED: '#BC5757',
    JobStatus.COMPLETED: '#00FF21',
    JobStatus.DONE: '#238802',
    JobStatus.FAILED: '#FF0000',
    JobStatus.KILLED: '#111111'
}

job_minor_status_palette = {
    "AncestorDepth Not Found": '#BAA312',
    'Application Finished With Errors': '#BC2133',
    'BK Input Data Not Available': '#E6D600',
    'Can not get Active and Banned Sites from JobDB': '#84CBFF',
    'Chosen site is not eligible': '#B4A243',
    'Error Sending Staging Request': '#B4A243',
    'Exceeded Maximum Dataset Limit (100)': '#BA5C9D',
    'Exception During Execution': '#AA240C',
    'Execution Complete': '#338B39',
    'Failed to access database': '#FFE267',
    'File Catalog Access Failure': '#FF8000',
    'Illegal Job JDL': '#D96C00',
    'Impossible Site + InputData Requirement': '#BDA822',
    'Impossible Site Requirement': '#F87500',
    'Input Data Not Available': '#2822A6',
    'Input Data Resolution': '#FFBE94',
    'Input Sandbox Download': '#586CFF',
    'Input data contains //': '#AB7800',
    'Input data not correctly specified': '#6812D6',
    'Job Wrapper Initialization': '#FFFFCC',
    'Job has exceeded maximum wall clock time': '#FF33CC',
    'Job has insufficient disk space to continue': '#33FFCC',
    'Job has reached the CPU limit of the queue': '#AABBCC',
    'No Ancestors Found For Input Data': '#BDA544',
    'No candidate sites available': '#E2FFBC',
    'No eligible sites for job': '#A8D511',
    'Parameter not found': '#FFB80C',
    'Pending Requests': '#52FF4F',
    'Received Kill signal': '#FF312F',
    'Socket read timeout exceeded': '#B400FE',
    'Stalled': '#FF655E',
    'Uploading Job Outputs': '#FE8420',
    'Watchdog identified this job as stalled': '#FFCC99'
}

miscelaneous_pallette = {
    'Others': '#666666',
    'NoLabels': '#0025AD',
    'Total': '#00FFDC',
    'Default': '#FDEE65'
}

country_palette = {
    'France': '#73C6BC',
    'UK': '#DCAF8A',
    'Spain': '#C2B0E1',
    'Netherlands': '#A9BF8E',
    'Germany': '#800000',
    'Russia': '#00514A',
    'Italy': '#004F00',
    'Switzerland': '#433B00',
    'Poland': '#528220',
    'Hungary': '#825CE2',
    'Portugal': '#009182',
    'Turkey': '#B85D00'
}


class Palette(object):

  def __init__(self, palette={}, colors=[]):

    self.palette = country_palette
    self.palette.update(job_status_palette)
    self.palette.update(miscelaneous_pallette)
    self.palette.update(job_minor_status_palette)

  def setPalette(self, palette):
    self.palette = palette

  def setColor(self, label, color):
    self.palette[label] = color

  def addPalette(self, palette):
    self.palette.update(palette)

  def getColor(self, label):

    if label in self.palette.keys():
      return self.palette[label]
    else:
      return self.generateColor(label)

  def generateColor(self, label):

    myMD5 = hashlib.md5()
    myMD5.update(label.encode())
    hexstring = myMD5.hexdigest()
    color = "#" + hexstring[:6]
    return color
