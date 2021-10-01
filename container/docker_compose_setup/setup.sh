envsubst < env_files/env_docker-compose.yml > docker-compose.yml
envsubst < env_files/env_setupMySQL.sql > resources/setupMySQL.sql
envsubst < env_files/env_install.cfg > resources/install.cfg

docker-compose up -d --build

docker exec -it dirac_devbox bash -c "cd /resources/ && chmod +x setupDIRAC.sh && ./setupDIRAC.sh"

sleep 1m

docker exec -it mysqldb bash -c "mysql -p$root_pass -uroot < /resources/setupMySQL.sql"
