
init:
	helm repo add splunk https://splunk.github.io/splunk-operator/
	helm repo update
	kubectl create namespace splunk-operator

install:
	helm install splunk-operator splunk/splunk-operator -n splunk-operator



