# DIRAC in docker containers

[WORK IN PROGRESS]

This documentation is a short, technical version of the official DIRAC documentation for what concerns developing using containers.

The **Dockerfile** file can be built in an image using (first place yourself in the $DEVROOT/DIRAC/container directory)::

  docker build --network host -t devbox .

where ``devbox`` is just a name, and ``--network host`` is not strictly necessary,
unless you are building from a location that forbids using the google DNS.

The created image can then be run using (for example):
```
docker run -h localhost -p 3424:3424 -it -v $DEVROOT/DIRAC:/opt/dirac/DIRAC -v $DEVROOT/etc:/opt/dirac/etc devbox bash
```
where the ``-p 3424:3424`` depends solely from which ports you wish to expose.
This example refers to the "HelloHandler" DIRAC service example that you can find in the developers tutorial
(http://dirac.readthedocs.io/en/integration/DeveloperGuide/AddingNewComponents/DevelopingServices/index.html)

The container as of now will create BOTH server and user credentials, including the (fake) CA,
so to work with it you should copy on your host the user certificate and key::
```
docker cp <container_ID>:/opt/dirac/user/client.key ~/.globus
docker cp <container_ID>:/opt/dirac/user/client.pem ~/.globus
```

You can then use these `client.{key, pem}` files for generating a proxy with dirac-proxy-init explicitly using the -K and -C options.
