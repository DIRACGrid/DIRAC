# DIRAC in Docker Compose Setup for Development
## Requirements
We assume that your system has `Docker` and `Docker Compose` running if not then follow [here](https://www.docker.com/).
## Initial Setup
Run the below commands for the setup of DIRAC Devbox, MySQL, and ElasticSearch:
```
source .env
export root_pass dirac_hostname
chmod +x setup.sh && ./setup.sh
```
**NOTE:-** &ensp;1) You must be inside this directory `<base_path>/DIRAC/container/docker_compose_setup` <br>
 &emsp;&emsp;&emsp;&emsp; 2) In order to change the DIRAC Hostname and MySQL password edit the `.env` file in the same folder.

## DIRAC Server Setup
Run the below commands:
```
docker exec -it dirac_devbox bash
cd ~/DiracInstallation/ && ./install_site.sh install.cfg
runsvstat /opt/dirac/startup/*
```

You will receive output similar to this:
```
Status of installed components:
[.......]
   Name                          Runit Uptime PID
=================================================
 1 Web_WebApp                    Run        6 653
 2 Configuration_Server          Run       70 352
 3 Framework_ComponentMonitoring Run       48 439
 4 Framework_SystemAdministrator Run       27 538

[root@dirac-dev DiracInstallation]# runsvstat /opt/dirac/startup/*
/opt/dirac/startup/Configuration_Server: run (pid 352) 123 seconds
/opt/dirac/startup/Framework_ComponentMonitoring: run (pid 439) 101 seconds
/opt/dirac/startup/Framework_SystemAdministrator: run (pid 538) 80 seconds
/opt/dirac/startup/Web_WebApp: run (pid 653) 59 seconds
```

## DIRAC Client Setup
Run the below commands:
```
~/DiracInstallation/setupDIRACClient.sh
```

After this you can access the WebAppDIRAC from your local machine at the following URLs:
```
http://localhost:8089
https://localhost:8443
```
In order to access the HTTPS client you wil need a `certificate` to get the certificate use the below command:
```
docker cp dirac_devbox:/root/.globus/certificate.p12 /<your-path>/certificate.p12
```
After you receive the `certificate.p12` file at your desired path you can upload it in our browser and use it to login to the HTTPS client.

## Making changes to DIRAC for Development and using DIRAC
After the installation always run this command in order to execute any DIRAC Commands:
```
docker exec -it dirac_devbox bash -c "source /opt/dirac/bashrc && bash"
```
If you make changes to DIRAC from your local host then run this inside the `dirac_devbox` container to see them effective:
```
cp -rn /localMount/DIRAC /opt/dirac/pro/
```

## Version Changes for Linux, MySQL, and ElasticSearch:
In order to change the versions you need to point the correct `dockerfiles` inside the `env_files/env_docker-compose.yml` file and the files are present in `dockerfiles/`.

## Bringing things down
In order to stop, remove, and bring all volume mounts down use this command:
```
docker-compose down --volume
```
