**********************
How to install Tornado
**********************


To install and run service on Tornado you should install DIRAC first. You can install DIRAC in the standard way. But for now you don't need to configure it and generate certificates. You just have to install DIRAC::

  mkdir -p /opt/dirac
  useradd dirac
  chown dirac:dirac /opt/dirac
  su - dirac
  cd /opt/dirac
  curl -O -L https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/dirac-install.py
  chmod +x dirac-install.py
  ./dirac-install.py -r v6r20p4 -t server

Once dirac installed you can add Tornado with the following steps:

***********************
Installing requirements
***********************
To install and compile some elements used by Tornado you may install some packages with ``yum``: ``python-devel``, ``m2crypto``,  ``gcc``. If you want to do some performance tests please check if ``nscd`` is running on your machine to avoid too many DNS query (on openstack, it is not enabled with SLC6).

Then you need to install Tornado and M2Crypto (for python), but not from official repo::

  pip --trusted-host=files.pythonhosted.org --trusted-host=pypi.org --trusted-host=pypi.python.org install  git+https://gitlab.com/chaen/m2crypto.git@tmpUntilSwigUpdated
  pip --trusted-host=files.pythonhosted.org --trusted-host=pypi.org --trusted-host=pypi.python.org install  git+https://github.com/chaen/tornado.git@iostreamConfigurable
  pip --trusted-host=files.pythonhosted.org --trusted-host=pypi.org --trusted-host=pypi.python.org install  git+https://github.com/chaen/tornado_m2crypto.git
  pip --trusted-host=files.pythonhosted.org --trusted-host=pypi.org --trusted-host=pypi.python.org install  -r /opt/dirac/DIRAC/requirements.txt

***********************
Adding Tornado to DIRAC
***********************

Save the DIRAC folder somewhere then clone my GithHub repo, then switch to branch "stage_toDIRAC_clean". You can run the setup.py if ``DIRAC.Core.Tornado`` is not detected by python::

  mv DIRAC DIRAC.old
  git clone https://github.com/louisjdmartin/DIRAC.git
  cd DIRAC
  git checkout stage_toDIRAC_clean
  python setup.py install



*********************
Generate Certificates
*********************
To use HTTPS your certificates must be generated using TLS standard, you can use following lines to generate them yourself::

  bash
  cd /tmp
  git clone https://github.com/chaen/DIRAC.git
  cd DIRAC
  git checkout rel-v6r20_FEAT_correctCA

  export DEVROOT=/tmp
  export SERVERINSTALLDIR=/opt/dirac/
  export CI_CONFIG=/tmp/DIRAC/tests/Jenkins/config/ci/

  source /tmp/DIRAC/tests/Jenkins/utilities.sh
  generateCA # automatic
  generateCertificates 365 # Certificates copied to /opt/dirac/etc/grid-security 
  generateUserCredentials 365 # Certificates generated at /opt/dirac/user -> copy to .globus and rename them userkey.pem and usercert.pem
  exit


**********************
Configuration (server)
**********************
Like in DISET, check your iptable to open some ports if needed !

Configuration is mostly the same as before, you just have to define ``Protocol`` to ``HTTPS`` inside the Services and add a new Section for tornado. You can use this example::

  LocalSite
  {
    Site = localhost
  }
  DIRAC
  {
    
    Setup = DeveloperSetup
    Setups
    {
      DeveloperSetup
      {
        Tornado = DevInstance
        Framework = DevInstance
      }
    }
    Security
    {
      UseServerCertificate=True
      CertFile = /opt/dirac/etc/grid-security/hostcert.pem
      KeyFile = /opt/dirac/etc/grid-security/hostkey.pem
    }
  }


  LocalInstallation
  {
    Setup = DeveloperSetup
  }


  Systems 
  {
    
    Tornado
    {
      DevInstance
      {
        
        Port = 4444 
      }
    }
    
    Framework
    {
      DevInstance
      {
        Databases
        {
          UserDB
          {
            Host = 127.0.0.1 #localhost
            User = root
            Password =
            DBName = dirac
          }
        }
        Services
        {
          User
          {
            # Use this handler to have a dummyService, can be used for testing without load a database
            #HandlerPath = DIRAC/FrameworkSystem/Service/DummyTornadoHandler.py
            Protocol = https
          }  
        }
      }
    }
  }
  Registry
  {
    # [Add your registry entry, like in DISET]
  }



**********************
Configuration (client)
**********************
Nothing change !
Define your URL as DIRAC service, but use https instead of dips::

  DIRAC
  { 
    Setup = DeveloperSetup
    Setups
    {
      DeveloperSetup
      {
        Framework = DevInstance
      }
    }
  }
  Systems
  {
    Framework
    {
      DevInstance
      {
        URLs
        {
          # DISET
          #User = dips://server:9135/Framework/User

          #TORNADO
          User = https://server:4444/Framework/User
        }
      }
    }
  }

****************
Start the server
****************

To start the server you must define ``OPENSSL_ALLOW_PROXY_CERTS`` and run ``DIRAC/TornadoServices/Scripts/tornado-start-all.py`` (or ``tornado-start-CS.py`` if you try to run a configuration server)::

  OPENSSL_ALLOW_PROXY_CERTS=1 python /opt/dirac/DIRAC/TornadoServices/scripts/tornado-start-all.py


You can now run DIRAC services. You can check the docstring of tests file (``DIRAC/test/Integration/TornadoServices`` and ``DIRAC/TornadoServices/test``) to know how to run tests.


*********************
Run performance tests
*********************
For performance test unset ``PYTHONOPTIMIZE`` if it is set in your environement::

  unset PYTHONOPTIMIZE


Then you have to start some clients (adapt the port)::

  cd /opt/dirac/DIRAC/test/Integration/TornadoServices
  multimech-run perf-test-ping -p 9000 -b 0.0.0.0

Modify first lines of ``DIRAC/TornadoServices/test/multi-mechanize/distributed-test.py`` and ``DIRAC/TornadoServices/test/multi-mechanize/plot-distributed-test.py`` (follow instruction of each files)

On the server start ``DIRAC/test/Integration/TornadoServices/getCPUInfos`` (redirect output to a file)

Run ``distributed-test.py [NameOfYourTest]`` at the end of execution, the command to plot is given. Before executing command, copy output of ``getCPUInfos`` on ``/tmp/results.txt`` (on your local machine).