============================
dirac-cert-convert.sh
============================

  From a p12 file, obtain the pem files with 
  the right access rights. Needed to obain a proxy.
  Creates the necessary directory, $HOME/.globus,
  if needed. Backs-up old pem files if any are found. 

Usage::

     dirac-cert-convert.sh CERT_FILE_NAME

Arguments::

  CERT_FILE_NAME:       Path to the p12 file.
