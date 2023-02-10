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
        plotStub = f"Z:{base64.urlsafe_b64encode(zlib.compress(DEncode.encode(plotRequest), 9)).decode()}"
    elif not gForceRawEncoding:
        plotStub = f"S:{base64.urlsafe_b64encode(DEncode.encode(plotRequest))}"
    else:
        plotStub = f"R:{DEncode.encode(plotRequest)}"
    # If thumbnail requested, use plot as thumbnail, and generate stub for plot without one
    extraArgs = plotRequest["extraArgs"]
    if "thumbnail" in extraArgs and extraArgs["thumbnail"]:
        thbStub = plotStub
        extraArgs["thumbnail"] = False
        if compress:
            plotStub = f"Z:{base64.urlsafe_b64encode(zlib.compress(DEncode.encode(plotRequest), 9)).decode()}"
        elif not gForceRawEncoding:
            plotStub = f"S:{base64.urlsafe_b64encode(DEncode.encode(plotRequest)).decode()}"
        else:
            plotStub = f"R:{DEncode.encode(plotRequest).decode()}"
    return S_OK({"plot": plotStub, "thumbnail": thbStub})


def extractRequestFromFileId(fileId):
    stub = fileId[2:]
    compressType = fileId[0]
    if compressType == "Z":
        gLogger.info("Compressed request, uncompressing")
        try:
            # Encoding is only required for Python 2 and can be removed when Python 2 support is no longer needed
            stub = base64.urlsafe_b64decode(stub.encode())
        except Exception as e:
            gLogger.error("Oops! Plot request is not properly encoded!", str(e))
            return S_ERROR(f"Oops! Plot request is not properly encoded!: {str(e)}")
        try:
            stub = zlib.decompress(stub)
        except Exception as e:
            gLogger.error("Oops! Plot request is invalid!", str(e))
            return S_ERROR(f"Oops! Plot request is invalid!: {str(e)}")
    elif compressType == "S":
        gLogger.info("Base64 request, decoding")
        try:
            stub = base64.urlsafe_b64decode(stub)
        except Exception as e:
            gLogger.error("Oops! Plot request is not properly encoded!", str(e))
            return S_ERROR(f"Oops! Plot request is not properly encoded!: {str(e)}")
    elif compressType == "R":
        # Do nothing, it's already uncompressed
        pass
    else:
        gLogger.error("Oops! Stub type is unknown", compressType)
        return S_ERROR(f"Oops! Stub type '{compressType}' is unknown :P")
    plotRequest, stubLength = DEncode.decode(stub)
    if len(stub) != stubLength:
        gLogger.error("Oops! The stub is longer than the data :P")
        return S_ERROR("Oops! The stub is longer than the data :P")
    return S_OK(plotRequest)
