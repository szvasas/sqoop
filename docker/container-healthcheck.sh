#!/bin/bash

EXITED_STATUS='"exited"'
HEALTHY_STATUS='"healthy"'
overallStatus=0
containers=(sqoop_mysql_container \
                   sqoop_postgres_container \
                   sqoop_mssql_container \
                   sqoop_cubrid_container \
                   sqoop_oracle_container \
                   sqoop_db2_container)

for container in ${containers[@]}; do
    containerStatus=`docker inspect --format='{{json .State.Status}}' $container`
    healthStatus=`docker inspect --format='{{json .State.Health.Status}}' $container`
    echo "$container: $containerStatus/$healthStatus"
    if [ $containerStatus == $EXITED_STATUS ] || [ $healthStatus != $HEALTHY_STATUS ]
    then
        overallStatus=1
    fi
done

exit $overallStatus