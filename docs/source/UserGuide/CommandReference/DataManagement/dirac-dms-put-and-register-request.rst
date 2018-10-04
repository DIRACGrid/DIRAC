==================================
dirac-dms-put-and-register-request
==================================

create and put 'PutAndRegister' request with a single local file

  warning: make sure the file you want to put is accessible from DIRAC production hosts,
           i.e. put file on network fs (AFS or NFS), otherwise operation will fail!!!

Usage::

 dirac-dms-put-and-register-request [option|cfgfile] requestName LFN localFile targetSE

Arguments::

 requestName: a request name
         LFN: logical file name   localFile: local file you want to put
    targetSE: target SE
