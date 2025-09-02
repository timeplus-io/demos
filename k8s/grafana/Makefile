NS=grafana


init:
	helm repo add grafana https://grafana.github.io/helm-charts
	helm repo update
	kubectl create namespace $(NS)

install:
	helm install grafana grafana/grafana \
		--namespace grafana \
		--set persistence.enabled=true \
		--set persistence.size=2Gi \
		--set adminPassword=Password!

show_password:
	kubectl get secret --namespace $(NS) grafana -o jsonpath="{.data.admin-password}" | base64 --decode

forward:
	kubectl port-forward --namespace $(NS) svc/grafana 3000:80

expose:
	kubectl patch svc grafana --namespace $(NS) -p '{"spec": {"type": "LoadBalancer"}}'

upgrade:
	helm upgrade grafana grafana/grafana \
		--namespace $(NS) \
		--values values.yaml