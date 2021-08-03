# Instruction manual for running integration tests on GitHub Actions, GitLab CI/CD, or on local, development box.

The scripts in this folder aims to automate integration tests for DIRAC, either in CI tools like GitHub Actions and GitLab CI/CD, or on any local computer fulfilling the prerequisites.

The main script `run_docker_setup.sh` achieves this by setting up Docker containers for MySQL, ElasticSearch, DIRAC server and DIRAC client and runs the predefined pytests in `DIRAC/tests/Integration/all_integration_*_tests.sh`.


## Running tests on CI tools

By default, the full CI job is executed on each push to the remote. Server and client installation logs, as well as server and client test logs are saved as artifacts for 7 days. 

The default test environment can be overridden with manual pipelines and following the instructions in [Overriding default configuration](#overriding-default-configuration). 

*NOTE*: The version of the Docker executor must be v18, or it might not be compatible with the Docker-In-Docker image 18-dind, which for Gitlab CI/CD is specified in `.gitlab-ci.yml`.

## Running tests locally
To run a test locally using the default configuration, make sure that the local computer fulfills the requirements [below](#system-requirements), and simply execute `run_docker_setup.sh` with *bash*. 

After the test process has been run, the server and client installation logs, as well as the server/client test logs can be retrieved to $PWD from the docker containers using `source utils.sh && getLogs`.

The command `docker-compose -f tests/CI/docker-compose.yml down -v` stops and removes the spawned containers (cleanup). 

### System requirements

The officially supported operating system is CERN Centos 7 ([CC7](http://linux.web.cern.ch/linux/centos7/)), should you use another Linux distribution, please install the dependencies according to your distribution. 

The following dependencies/softwares are required:
* Docker v18+ (lower version should work, but no promises are made). 
* Docker-Compose v2.4+ format

```
sudo yum install -y docker
sudo systemctl enable docker
sudo systemctl start docker
sudo groupadd docker
sudo usermod -aG docker $USER    # So that you don't need sudo docker

sudo yum install -y python-pip python-devel libffi-devel openssl-devel gcc glibc-devel make curl
pip install docker-compose
```

## Test environment configuration

The environment variables injected into the test environment are located in the CONFIG file. 

The configuration will automatically detect if it is in GitLab CI, and will automatically configure to retrieve TestCode and source from the current branch, as well as setting the release to the one in `__init__.py`. 

See the CONFIG file for specific details for every variable. 

### Overriding default configuration

The environment variables are easily overridable by exporting the variable with the DEFAULT_ name prefix stripped, with whichever value you wish. Do this _before_ running `run_docker_setup.sh` if running local testing, or inject custom variables in the GitLab CI Pipeline. 

Example:
I want to change the version of MySQL to 8.0 (from default 5.7). The corresponding variable in CONFIG is `MYSQL_VER=mysql:5.7`. Export `MYSQL_VER=mysql:8.0` to override defaults.

#### Using client or server specific settings

One might for instance like to set the DIRAC_RELEASE variable to be different for the client, as compared to the server, to test if v(n-1) is compatible with v(n). This is achieved by first exporting the variable prefixed with CLIENT_ or SERVER_ before running `run_docker_setup.sh` to override the default. 

Example:
Default setting for DIRAC_RELEASE is "unset" (not passed to the server/client containers), by exporting CLIENT_DIRAC_RELEASE=v7r1-pre1, it will override the default value for the client, and the default value will still be used for the server. 

The priority for configuration is as follows:
1. Check if CLIENT_ (or SERVER_) prefixed variable exists
2. Check if unprefixed variable exists
3. Else use default variable

#### Unsetting default values

Sometimes it may be preferable to disable something that is enabled by default. For instance in Gitlab the local source is installed by default on top of the latest release. By overriding the value as "unset", the variable will not be passed along to the server/client docker containers, and thus in this specific case, only the DIRAC tarball will be installed. 

Example:

``` shell
DEFAULT_ALTERNATIVE_MODULES=/afs/cern.ch/user/a/alu/local/dirac
# I don't want to install the latest local source
export CLIENT_ALTERNATIVE_MODULES=unset
# Don't install local source on client
export ALTERNATIVE_MODULES=unset
# Don't install local source on neither client nor server
```


### Using local test repository and local source

The pytests and DIRAC installations scripts (commonly referred to as TestCode) can be fetched from a local source instead of a remote Git. 
To do this, export the `TESTREPO` variable as the absolute file path of the root of the local DIRAC repository. 

DIRAC can also be (partially) installed from a local source by overriding the `ALTERNATIVE_MODULES` with the absolute file path of the root of the local DIRAC repository. 

The default for Gitlab CI is to use the local source (current repo/branch) for both TestCode and for DIRAC installation. 

