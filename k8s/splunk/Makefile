VERSION = 3.1.0
NAMESPACE = splunk-operator
# must exist on the target cluster (EKS: timeplus-storage-class / gp3; GKE: standard-rwo)
STORAGE_CLASS = timeplus-storage-class
# accepts the Splunk General Terms (https://www.splunk.com/en_us/legal/splunk-general-terms.html);
# the operator refuses to deploy instances without it
GENERAL_TERMS = --accept-sgt-current-at-splunk-com

init:
	helm repo add splunk https://splunk.github.io/splunk-operator/
	helm repo update
	kubectl create namespace $(NAMESPACE)

# CRDs are no longer bundled in the helm chart (too large) and must be applied separately
install_crds:
	kubectl apply --server-side -f https://github.com/splunk/splunk-operator/releases/download/$(VERSION)/splunk-operator-crds.yaml

install: install_crds
	helm install splunk-operator splunk/splunk-operator --version $(VERSION) -n $(NAMESPACE) \
		--set splunkOperator.persistentVolumeClaim.storageClassName=$(STORAGE_CLASS) \
		--set splunkOperator.splunkGeneralTerms="$(GENERAL_TERMS)"

upgrade:
	helm upgrade splunk-operator splunk/splunk-operator --version $(VERSION) -n $(NAMESPACE) \
		--set splunkOperator.persistentVolumeClaim.storageClassName=$(STORAGE_CLASS) \
		--set splunkOperator.splunkGeneralTerms="$(GENERAL_TERMS)"

apply:
	kubectl apply -f splunk-standalone.yaml
	kubectl apply -f hec_service.yaml

status:
	kubectl get standalone,pods,svc -n $(NAMESPACE)

show_password:
	kubectl get secret splunk-single-standalone-secret-v1 -n $(NAMESPACE) -o jsonpath='{.data.password}' | base64 --decode

uninstall:
	kubectl delete -f hec_service.yaml --ignore-not-found
	kubectl delete -f splunk-standalone.yaml --ignore-not-found
	helm uninstall splunk-operator -n $(NAMESPACE)
