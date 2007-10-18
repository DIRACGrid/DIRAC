#################################
# Author : Vincent Garonne      #
# Mail : garonne@cppm.in2p3.fr  #
# file : Pfn.py                 #
# date : 21/11/03               ##################################
# description :  Module to (un)parse physical file name          #
# modified: A.Tsaregorodtsev, 06.06.2005                         #
##################################################################

from DIRAC  import S_OK, S_ERROR
import string, os
from urlparse import *

def pfnparse (pfn):
    result    = {}
    protocols = ['http', 'ftp', 'file']
    #protocols = []
    protocol, host, path, params, query, fragment = urlparse(pfn)
    result['protocol'] = protocol
    if protocol not in protocols:
        protocol, host, path, params, query, fragment = urlparse (urljoin ("http:",path))
    if string.find(host,':')!=-1:
        result['port'] = string.split (host,':')[1]
        result['host'] = string.split (host,':')[0]
        if not result['port']:
          result['host'] = result['host']+":"
    else:
        result['port'] = ''
        result['host'] = host

    #result['path'] = path[0:string.rfind(path,'/')+1]
    #result['file'] = path[string.rfind(path,'/')+1:len(path)]
    result['path'] = os.path.dirname(path)
    result['file'] = os.path.basename(path)
    return S_OK(result)

def pfnunparse (pfn):

  path = pfn['path']+'/'+pfn['file']
  path = path.replace('//','/')

  if pfn['port']:
      host =pfn['host']+':'+ pfn['port']
  else:
      host = pfn['host']

  if host:
    result = pfn['protocol']+"://"+host+path
  else:
    result = pfn['protocol']+":"+path

  #result = urlunparse ((pfn['protocol'],host, path, None, None, None))
  #result = result.replace(" ","")
  return S_OK(result)
