# https
curl -k -X POST \
  -H "Authorization: Splunk token" \
  -H "Content-Type: application/json" \
  -d '{"event": "Hello World", "sourcetype": "manual"}' \
  https://localhost:8088/services/collector

#http
curl -X POST \
  -H "Authorization: Splunk token" \
  -H "Content-Type: application/json" \
  -d '{"event": "Hello World", "sourcetype": "manual"}' \
  http://localhost:8088/services/collector