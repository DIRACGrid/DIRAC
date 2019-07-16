# Instruction manual for GitLab CI tests

The scripts in this folder aims to automate integration tests for DIRAC either in GitLab CI, or on any local computer fulfilling the prerequisites.

The main script `run_integration_docker.sh` achieves this by setting up Docker containers for MySQL, ElasticSearch, DIRAC server and DIRAC client and runs the predefined pytests in DIRAC/tests/Integration/all_integration_*_tests.sh. 


## Running tests on GitLab CI

By default, the full CI job is executed on each push to the remote. Server and client installation logs, as well as server and client test logs are saved as artifacts for 7 days. 

The default test environment can be overridden with manual pipelines and following the instructions in [Overriding default configuration](#overriding-default-configuration). 


## Running tests locally
To run a test locally using the default configuration, make sure that the local computer fulfills the requirements [below](#system-requirements), and simply execute `run_integration_tests.sh` with *bash*. 

After the test process has been run, the server and client installation logs, as well as the server/client test logs can be retrieved to $PWD from the docker containers using `source utils.sh && getLogs`.

The `cleanup.sh` script is a convenience script to take down and remove the created Docker containers and removes temporary files. 

### System requirements

The officially supported operating system is CERN Centos 7 ([CC7](http://linux.web.cern.ch/linux/centos7/)), should you use another Linux distribution, please install the dependencies according to your distribution. 

The following dependencies/softwares are required:
* Docker v18+ (lower version should work, but no promises are made).
* Docker-Compose v2.4+ 

```
sudo yum install -y docker
sudo systemctl enable docker
sudo systemctl start docker

sudo yum install -y python-pip python-devel libffi-devel openssl-devel gcc glibc-devel make curl
pip install docker-compose
```

## Test environment configuration

The environment variables injected into the test environment are located in the CONFIG file. 

The configuration will automatically detect if it is in GitLab CI, and will automatically configure to retrieve TestCode from the current branch. 

See the CONFIG file for specific details for every variable. 

### Overriding default configuration

The environment variables are easily overridable by exporting the variable with the DEFAULT_ name prefix stripped, with whichever value you wish. Do this _before_ running `run_integration_docker.sh` if running local testing, or inject custom variables in the GitLab CI Pipeline. 

Example:
I want to change the version of MySQL to 8.0 (from default 5.7). The corresponding variable in CONFIG is `DEFAULT_MYSQL_VER=5.7`. Export `MYSQL_VER=8.0` to override defaults. 

### Using local test repository and local source

The pytests and DIRAC installations scripts (located in DIRAC/tests/Jenkins) can be fetched from a local source instead of a remote Git. 
To do this, export the `TESTREPO` variable as the absolute file path of the root of the local DIRAC repository. 

DIRAC can also be (partially) installed from a local source by overriding the `ALTERNATIVE_MODULES` with the absolute file path of the root of the local DIRAC repository. 

### LHCb DIRAC 

LHCb DIRAC can be automatically installed alongside DIRAC, by exporting the variables `LHCBDIRACBRANCH` and `LHCBTESTBRANCH`, which will prompt the script to install LHCb DIRAC as well. 

This feature is currently untested and may not work as intended. 

There are plans to also run pytests from LHCb DIRAC, but they have not been implemented as of now. 
