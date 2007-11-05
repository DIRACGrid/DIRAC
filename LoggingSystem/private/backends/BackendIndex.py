
from DIRAC.LoggingSystem.private.backends.PrintBackend import PrintBackend
from DIRAC.LoggingSystem.private.backends.RemoteBackend import RemoteBackend
from DIRAC.LoggingSystem.private.backends.FileBackend import FileBackend
from DIRAC.LoggingSystem.private.backends.StdErrBackend import StdErrBackend

gBackendIndex = { 'stdout' : PrintBackend,
                  'stderr' : StdErrBackend,
                  'server' : RemoteBackend,
                  'file'   : FileBackend
                }
