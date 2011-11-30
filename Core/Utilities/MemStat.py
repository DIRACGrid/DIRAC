import os
def VmB( vmKey ):
  __memScale = {'kB': 1024.0, 'mB': 1024.0 * 1024.0, 'KB': 1024.0, 'MB': 1024.0 * 1024.0}
  __vmKeys = [ 'VmPeak:', 'VmSize:', 'VmLck:', 'VmHWM:', 'VmRSS:', 'VmData:', 'VmStk:', 'VmExe:', 'VmLib:', 'VmPTE:',
               'VmPeak', 'VmSize', 'VmLck', 'VmHWM', 'VmRSS', 'VmData', 'VmStk', 'VmExe', 'VmLib', 'VmPTE'  ]
  if vmKey not in __vmKeys:
    return 0
  procFile = '/proc/%d/status' % os.getpid()
   # get pseudo file  /proc/<pid>/status
  try:
    myFile = open( procFile )
    value = myFile.read()
    myFile.close()
  except:
    return 0.0  # non-Linux?
   # get vmKey line e.g. 'VmRSS:  9999  kB\n ...'
  i = value.index( vmKey )
  value = value[i:].split( None, 3 )  # whitespace
  if len( value ) < 3:
    return 0.0  # invalid format?
   # convert Vm value to bytes
  return float( value[1] ) * __memScale[value[2]]
