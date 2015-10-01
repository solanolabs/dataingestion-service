set -x
set -e
cf login -a $CF_URL --skip-ssl-validation -u ${CF_USERNAME} -p ${CF_PASSWORD} -o ${ORG} -s ${SPACE}
cf push $SERVICE -f manifest-integration.yml
