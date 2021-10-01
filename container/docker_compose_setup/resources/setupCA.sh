mkdir -p ~/caUtilities/ && cd ~/caUtilities/
curl -O -L https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/tests/Jenkins/utilities.sh
curl -O -L https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/tests/Jenkins/config/ci/openssl_config_ca.cnf
curl -O -L https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/tests/Jenkins/config/ci/openssl_config_host.cnf
curl -O -L https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/tests/Jenkins/config/ci/openssl_config_user.cnf
export SERVERINSTALLDIR=/opt/dirac
export CI_CONFIG=~/caUtilities/
source utilities.sh
generateCA
generateCertificates 365
generateUserCredentials 365
