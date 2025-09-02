NS=opensearch

init:
	helm repo add opensearch https://opensearch-project.github.io/helm-charts/
	helm repo update
	kubectl create namespace $(NS)

install:
	helm install opensearch opensearch/opensearch \
		--namespace $(NS) \
		--values opensearch-values.yaml \
		--version 2.15.0

install_dashboard:
	helm install opensearch-dashboards opensearch/opensearch-dashboards \
		--namespace $(NS) \
		--values opensearch-dashboard-values.yaml \
		--version 2.15.0

