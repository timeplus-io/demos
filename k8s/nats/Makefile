init:
	helm repo add nats https://nats-io.github.io/k8s/helm/charts/
	helm repo update


install:
	helm install nats nats/nats -n nats -f values.yaml
