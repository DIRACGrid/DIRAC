"""
  This module contains methods for the validation of production definitions
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id $"

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog


class ProdValidator(object):

  def __init__(self):
    self.transClient = TransformationClient()

  def checkTransStatus(self, transID):
    """ Check if the status of the transformation is valid for the transformation to be added to a production.
        New is the only valid status

    :param int transID: the TransformationID
    """
    res = self.transClient.getTransformationParameters(transID, 'Status')
    if not res['OK']:
      return res
    status = res['Value']
    if status != 'New':
      return S_ERROR("checkTransStatus failed : Invalid transformation status: %s" % status)

    return S_OK()

  def checkTransDependency(self, transID, parentTransID):
    """ Check if the transformation and the parent transformation are linked

    :param int transID: the TransformationID
    :param int parentTransID: the parent TransformationID
    """
    res = self.transClient.getTransformationMetaQuery(transID, 'Input')
    if not res['OK']:
      return res
    inputquery = res['Value']
    if not inputquery:
      return S_ERROR("No InputMetaQuery defined for transformation %s" % transID)

    res = self.transClient.getTransformationMetaQuery(parentTransID, 'Output')
    if not res['OK']:
      return res
    parentoutputquery = res['Value']
    if not parentoutputquery:
      return S_ERROR("No OutputMetaQuery defined for parent transformation %s" % parentTransID)

    # Check the matching between inputquery and parent outputmeta query
    # Currently very simplistic: just support expression with "=" and "in" operators
    gLogger.notice("Applying checkMatchQuery")
    res = self.checkMatchQuery(inputquery, parentoutputquery)

    if not res['OK']:
      gLogger.error("checkMatchQuery failed")
      return res
    if not res['Value']:
      return S_ERROR("checkMatchQuery result is False")

    return S_OK()

  def checkMatchQuery(self, mq, mqParent):
    """ Check the logical intersection between the two metaqueries

    :param dict mq: a dictionary of the MetaQuery to be checked against the mqParent
    :param dict mqParent: a dictionary of the parent MetaQuery to be checked against the mq
    """
    # Get the metadata types defined in the catalog
    catalog = FileCatalog()
    res = catalog.getMetadataFields()
    if not res['OK']:
      gLogger.error("Error in getMetadataFields: %s" % res['Message'])
      return res
    if not res['Value']:
      gLogger.error("Error: no metadata fields defined")
      return res

    MetaTypeDict = res['Value']['FileMetaFields']
    MetaTypeDict.update(res['Value']['DirectoryMetaFields'])

    res = self.checkformatQuery(mq)
    if not res['OK']:
      return res
    MetaQueryDict = res['Value']

    res = self.checkformatQuery(mqParent)
    if not res['OK']:
      return res
    ParentMetaQueryDict = res['Value']

    for meta, value in MetaQueryDict.items():
      if meta not in MetaTypeDict:
        msg = 'Metadata %s is not defined in the Catalog' % meta
        return S_ERROR(msg)
      mtype = MetaTypeDict[meta]
      if mtype.lower() not in ['varchar(128)', 'int', 'float']:
        msg = 'Metatype %s is not supported' % mtype.lower()
        return S_ERROR(msg)
      if meta not in ParentMetaQueryDict:
        msg = 'Metadata %s is not in parent transformation query' % meta
        return S_ERROR(msg)
      if self.compareValues(value, ParentMetaQueryDict[meta]):
        continue
      else:
        msg = "Metadata values %s do not match with %s" % (value, ParentMetaQueryDict[meta])
        gLogger.error(msg)
        return S_OK(False)

    return S_OK(True)

  def checkformatQuery(self, MetaQueryDict):
    """ Check the format query and transform all dict values in dict for uniform treatment

    :param dict MetaQueryDict: a dictionary of the MetaQuery
    """
    for meta, value in MetaQueryDict.items():
      values = []
      if isinstance(value, dict):
        operation = list(value)[0]
        if operation not in ['=', 'in']:
          msg = 'Operation %s is not supported' % operation
          return S_ERROR(msg)
        else:
          if not isinstance(list(value.values())[0], list):
            MetaQueryDict[meta] = {"in": list(value.values())}
      else:
        values.append(value)
        MetaQueryDict[meta] = {"in": values}

    return S_OK(MetaQueryDict)

  def compareValues(self, value, parentValue):
    """ Very simple comparison. To be improved

    :param dict value: a dictionary with meta data values to be compared with the parentValues
    :param dict parentValue: a dictionary with meta data parentValues be compared with values
    """
    return set(list(value.values())[0]).issubset(set(list(parentValue.values())[0])) or \
        set(list(parentValue.values())[0]).issubset(set(list(value.values())[0]))
