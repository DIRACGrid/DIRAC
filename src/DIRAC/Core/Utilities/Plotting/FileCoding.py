from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import base64
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import DEncode

gForceRawEncoding = False

try:
  import zlib
  gZCompressionEnabled = True
except ImportError as x:
  gZCompressionEnabled = False


def codeRequestInFileId(plotRequest, compressIfPossible=True):
  compress = compressIfPossible and gZCompressionEnabled
  thbStub = False
  if compress:
    plotStub = "Z:%s" % base64.urlsafe_b64encode(zlib.compress(DEncode.encode(plotRequest), 9)).decode()
  elif not gForceRawEncoding:
    plotStub = "S:%s" % base64.urlsafe_b64encode(DEncode.encode(plotRequest))
  else:
    plotStub = "R:%s" % DEncode.encode(plotRequest)
  # If thumbnail requested, use plot as thumbnail, and generate stub for plot without one
  extraArgs = plotRequest['extraArgs']
  if 'thumbnail' in extraArgs and extraArgs['thumbnail']:
    thbStub = plotStub
    extraArgs['thumbnail'] = False
    if compress:
      plotStub = "Z:%s" % base64.urlsafe_b64encode(zlib.compress(DEncode.encode(plotRequest), 9)).decode()
    elif not gForceRawEncoding:
      plotStub = "S:%s" % base64.urlsafe_b64encode(DEncode.encode(plotRequest)).decode()
    else:
      plotStub = "R:%s" % DEncode.encode(plotRequest).decode()
  return S_OK({'plot': plotStub, 'thumbnail': thbStub})


def extractRequestFromFileId(fileId):
  stub = fileId[2:]
  compressType = fileId[0]
  if compressType == 'Z':
    gLogger.info("Compressed request, uncompressing")
    try:
      stub = base64.urlsafe_b64decode(stub)
    except Exception as e:
      gLogger.error("Oops! Plot request is not properly encoded!", str(e))
      return S_ERROR("Oops! Plot request is not properly encoded!: %s" % str(e))
    try:
      stub = zlib.decompress(stub)
    except Exception as e:
      gLogger.error("Oops! Plot request is invalid!", str(e))
      return S_ERROR("Oops! Plot request is invalid!: %s" % str(e))
  elif compressType == 'S':
    gLogger.info("Base64 request, decoding")
    try:
      stub = base64.urlsafe_b64decode(stub)
    except Exception as e:
      gLogger.error("Oops! Plot request is not properly encoded!", str(e))
      return S_ERROR("Oops! Plot request is not properly encoded!: %s" % str(e))
  elif compressType == 'R':
    # Do nothing, it's already uncompressed
    pass
  else:
    gLogger.error("Oops! Stub type is unknown", compressType)
    return S_ERROR("Oops! Stub type '%s' is unknown :P" % compressType)
  plotRequest, stubLength = DEncode.decode(stub)
  if len(stub) != stubLength:
    gLogger.error("Oops! The stub is longer than the data :P")
    return S_ERROR("Oops! The stub is longer than the data :P")
  return S_OK(plotRequest)
