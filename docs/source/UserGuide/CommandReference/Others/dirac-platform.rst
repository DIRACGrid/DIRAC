==============
dirac-platform
==============

The *dirac-platform* script determines the "platform" of a certain node.
The platform is a string used to identify the minimal characteristics of the node,
enough to determine which version of DIRAC can be installed.

Invoked at any installation, so by the *dirac-install* script, and by the pilots.

On a RHEL 6 node, for example, the determined dirac platform is "Linux_x86_64_glibc-2.5"

Example::

   $ dirac-platform
   Linux_x86_64_glibc-2.5
