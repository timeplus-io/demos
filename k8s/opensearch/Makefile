NS=opensearch

init:
	helm repo add opensearch https://opensearch-project.github.io/helm-charts/
	helm repo update
	kubectl create namespace $(NS)

install:
	helm install opensearch opensearch/opensearch \
		--namespace $(NS) \
		--values opensearch-values.yaml \
		--version 3.2.1

upgrade:
	helm upgrade opensearch opensearch/opensearch \
		--namespace $(NS) \
		--values opensearch-values.yaml

install_dashboard:
	helm install opensearch-dashboards opensearch/opensearch-dashboards \
		--namespace $(NS) \
		--values opensearch-dashboard-values.yaml \
		--version 3.2.1

expose_dashboard:
	kubectl expose deployment opensearch-dashboards -n $(NS) --type=LoadBalancer --name=opensearch-dashboards-elb --port=5601 --target-port=5601