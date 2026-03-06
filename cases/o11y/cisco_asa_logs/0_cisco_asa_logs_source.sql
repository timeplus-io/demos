CREATE EXTERNAL STREAM IF NOT EXISTS cisco_observability.asa_logs_stream (
    message string
)
SETTINGS 
    type = 'kafka', 
    brokers = 'bootstrap.demo.us-west1.managedkafka.tpdemo2025.cloud.goog:9092', 
    topic = 'cisco_asa_logs', 
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    config_file='etc/kafka-config/client.properties',
    skip_ssl_cert_check = false,
    data_format='JSONEachRow', 
    one_message_per_row=true;
