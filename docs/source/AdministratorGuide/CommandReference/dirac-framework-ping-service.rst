===================================
dirac-framework-ping-service
===================================

  Ping the given DIRAC Service

Usage::

  dirac-framework-ping-service [option|cfgfile] ... System Service|System/Agent

Arguments::

  System:   Name of the DIRAC system (ie: WorkloadManagement)

  Service:  Name of the DIRAC service (ie: Matcher) 

Example::

  $ dirac-framework-ping-service WorkloadManagement MPIService
  {'OK': True,
   'Value': {'cpu times': {'children system time': 0.0,
                           'children user time': 0.0,
                           'elapsed real time': 8778481.7200000007,
                           'system time': 54.859999999999999,
                           'user time': 361.06999999999999},
             'host uptime': 4485212L,
             'load': '3.44 3.90 4.02',
             'name': 'WorkloadManagement/MPIService',
             'service start time': datetime.datetime(2011, 2, 21, 8, 58, 35, 521438),
             'service uptime': 85744,
             'service url': 'dips://dirac.in2p3.fr:9171/WorkloadManagement/MPIService',
             'time': datetime.datetime(2011, 3, 14, 11, 47, 40, 394957),
             'version': 'v5r12-pre9'},
   'rpcStub': (('WorkloadManagement/MPIService',
                {'delegatedDN': '/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar',
                 'delegatedGroup': 'dirac_user',
                 'skipCACheck': True,
                 'timeout': 120}),
               'ping',
               ())}

