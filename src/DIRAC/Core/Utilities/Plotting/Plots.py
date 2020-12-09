"""
It used used to create different plots.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from six import BytesIO
import errno
from DIRAC.Core.Utilities.Graphs import barGraph, lineGraph, pieGraph, qualityGraph, textGraph, histogram

from DIRAC import S_OK, S_ERROR


def checkMetadata(metadata):
  """
  :param dict metadata: it contains information which will used in the plot creation.
  """
  if 'span' in metadata:
    granularity = metadata['span']
    if 'starttime' in metadata:
      metadata['starttime'] = metadata['starttime'] - metadata['starttime'] % granularity
    if 'endtime' in metadata:
      metadata['endtime'] = metadata['endtime'] - metadata['endtime'] % granularity
  if 'limit_labels' not in metadata:
    metadata['limit_labels'] = 9999999


def generateNoDataPlot(fileName, data, metadata):
  """
  Tis generate an image with a specific error message.

  :param str fileName: name of the file
  :param list data: data
  :param dict metadata: metadata information
  """
  try:
    with open(fileName, "wb") as fn:
      text = "No data for this selection for the plot: %s" % metadata['title']
      textGraph(text, fn, metadata)
  except IOError as e:
    return S_ERROR(errno.EIO, e)
  return S_OK()


def generateErrorMessagePlot(msgText):
  """
  It creates a plot whith a specific error message

  :param str msgText: the text which will appear on the plot.
  :return: the plot.
  """
  fn = BytesIO()
  textGraph(msgText, fn, {})
  data = fn.getvalue()
  fn.close()
  return S_OK(data)


def generateTimedStackedBarPlot(fileName, data, metadata):
  """
  It is used to create a time based line plot.

  :param str fileName: the name of the file
  :param list data: the data which is used to create the plot
  :param dict metadata: extra information used to create the plot.
  """
  try:
    with open(fileName, "wb") as fn:
      checkMetadata(metadata)
      for key, value in (('sort_labels', 'sum'), ('legend_unit', '%')):
        if key not in metadata:
          metadata[key] = value
      barGraph(data, fn, **metadata)
  except IOError as e:
    return S_ERROR(errno.EIO, e)
  return S_OK()


def generateQualityPlot(fileName, data, metadata):
  """
  It is used to create 2D plots.

  :param str fileName: the name of the file
  :param list data: the data which is used to create the plot
  :param dict metadata: extra information used to create the plot.
  """
  try:
    with open(fileName, "wb") as fn:
      checkMetadata(metadata)
      metadata['legend'] = False
      # HACK: Pad a bit to the left until the proper padding is calculated
      maxKeyLength = max([len(key) for key in data])
      metadata['sort_labels'] = 'alpha'
      metadata['plot_left_padding'] = int(maxKeyLength * 2.5)
      qualityGraph(data, fn, **metadata)
  except IOError as e:
    return S_ERROR(errno.EIO, e)
  return S_OK()


def generateCumulativePlot(fileName, data, metadata):
  """
  It is used to create cumulativ plots.

  :param str fileName: the name of the file
  :param list data: the data which is used to create the plot
  :param dict metadata: extra information used to create the plot.
  """
  try:
    with open(fileName, "wb") as fn:
      checkMetadata(metadata)
      if 'sort_labels' not in metadata:
        metadata['sort_labels'] = 'last_value'
      lineGraph(data, fn, **metadata)
  except IOError as e:
    return S_ERROR(errno.EIO, e)
  return S_OK()


def generateStackedLinePlot(fileName, data, metadata):
  """
  It is used to create stacked line plot.

  :param str fileName: the name of the file
  :param list data: the data which is used to create the plot
  :param dict metadata: extra information used to create the plot.
  """
  try:
    with open(fileName, "wb") as fn:
      checkMetadata(metadata)
      for key, value in (('sort_labels', 'sum'), ('legend_unit', '%')):
        if key not in metadata:
          metadata[key] = value
      lineGraph(data, fn, **metadata)
  except IOError as e:
    return S_ERROR(errno.EIO, e)
  return S_OK()


def generatePiePlot(fileName, data, metadata):
  """
  It is used to create pie charts.

  :param str fileName: the nanme of the file
  :param list data: the data which is used to create the plot
  :param dict metadata: extra information used to create the plot.
  """
  try:
    with open(fileName, "wb") as fn:
      checkMetadata(metadata)
      pieGraph(data, fn, **metadata)
  except IOError as e:
    return S_ERROR(errno.EIO, e)
  return S_OK()


def generateHistogram(filename, data, metadata):
  """
  It is used to create histograms.

  :param str fileName: the nanme of the file
  :param list data: the data which is used to create the plot
  :param dict metadata: extra information used to create the plot.
  """
  try:
    with open(filename, "wb") as fn:
      checkMetadata(metadata)
      histogram(data, fn, **metadata)
  except IOError as e:
    return S_ERROR(errno.EIO, e)
  return S_OK()
