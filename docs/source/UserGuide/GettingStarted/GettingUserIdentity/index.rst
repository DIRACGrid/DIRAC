==================================
Getting User Identity
==================================

To start working with the Grid in general and with DIRAC in particular, the user should join some
grid Virtual Organization and obtain a Grid Certificate. The procedure to obtain the Grid Certificate
depends on the user's national Certification Authority (CA). The certificate is usually obtained via a 
Web interface and is downloaded into the user's Web Browser. To be used with the Grid client software,
the certificate should be exported from the Browser into a file in p12 format. After that the certificate
should be converted into the pem format and stored in the user home directory. If the DIRAC client software 
is available, the conversion can be done with the following DIRAC command::

  dirac-cert-convert.sh <cert_file.p12> 

The user will be prompted for the password used while exporting the certificate and for the pass phrase
to be used with the user's private key. Do not forget it ! 

Registration with DIRAC
-------------------------

Users are always working in the Grid as members of some User Community. Therefore, every user must be registered
with the Community DIRAC instance. You should ask the DIRAC administrators to do that, the procedure can
be different for different communities.

Once registered, a user becomes a member of one of the DIRAC user groups. The membership in the group
determines the user rights for various Grid operations. Each DIRAC installation defines a default user
group to which the users are attributed when the group is not explicitly specified.

Proxy initialization
-----------------------

Before a user can work with DIRAC, the user's certificate proxy should be initialized and
uploaded to the DIRAC ProxyManager Service. This is achieved with a simple command::

  proxy-init
  
In this case the user proxy with the default DIRAC group will be generated and uploaded.
If another non-default user group is needed, the command becomes::

  proxy-init -g <user_group> 
  
where ''user_group'' is the desired DIRAC group name for which the user is entitled.  


