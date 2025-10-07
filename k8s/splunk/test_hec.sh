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

# test with ELB
curl -X POST \
  -H "Authorization: Splunk f50aef7d-bd49-4ff3-90f9-d8ac54ecbe37" \
  -H "Content-Type: application/json" \
  -d '{"event": "Hello World 3", "sourcetype": "manual"}' \
http://35.230.87.146:8088/services/collector