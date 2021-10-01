mkdir ~/.globus/
cp /opt/dirac/user/client.pem ~/.globus/usercert.pem
cp /opt/dirac/user/client.key ~/.globus/userkey.pem
source /opt/dirac/bashrc
source /opt/dirac/bashrc
dirac-proxy-init -g dirac_admin
cd ~/.globus/
openssl pkcs12 -export -out certificate.p12 -inkey userkey.pem -in usercert.pem
cd
