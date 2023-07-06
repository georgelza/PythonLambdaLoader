. ./.pws

export kafka_bootstrap_port=9092
export kafka_topic_name=SNDBX_TFM_engineResponse

########################################################################
# Golang  Examples : https://developer.confluent.io/get-started/go/

### Confluent Cloud Cluster
#export kafka_bootstrap_servers= -> see .pws
export kafka_security_protocol=SASL_SSL
export kafka_sasl_mechanisms=PLAIN
#export kafka_sasl_username= -> see .pws
#export kafka_sasl_password= -> see .pws

export flushcap=2000
export reccap=10

python3 local_conf_readline_loader.py


# https://docs.confluent.io/platform/current/app-development/kafkacat-usage.html
# kafkacat -b localhost:9092 -t SNDBX_AppLab
