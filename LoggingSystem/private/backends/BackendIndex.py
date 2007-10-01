
from DIRAC.LoggingSystem.private.backends.PrintBackend import PrintBackend
from DIRAC.LoggingSystem.private.backends.RemoteBackend import RemoteBackend
from DIRAC.LoggingSystem.private.backends.FileBackend import FileBackend

gBackendIndex = { 'stdout' : PrintBackend,
                  'server' : RemoteBackend,
                  'file'   : FileBackend
                }
