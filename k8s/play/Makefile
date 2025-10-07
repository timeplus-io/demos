STORAGE_CLASS=$(shell kubectl get storageclass -o jsonpath='{.items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")].metadata.name}')
NS=play
RELEASE=timeplus
VERSION=8.0.6
PLATFORM=gcp

.PHONY: logs

init:
	kubectl create ns $(NS) 

install:
	helm -n $(NS) install \
		-f ./values_$(PLATFORM).yaml \
		$(RELEASE) ./timeplus-enterprise-v${VERSION}.tgz

dryrun:
	helm -n $(NS) install \
		-f ./values_$(PLATFORM).yaml \
		--dry-run \
		$(RELEASE) ./timeplus-enterprise-v${VERSION}.tgz

uninstall:
	helm -n $(NS) uninstall $(RELEASE)

forward:
	kubectl port-forward svc/timeplus-appserver 8000:8000 -n $(NS) --address 0.0.0.0

clean:
	-kubectl delete pvc timeplusd-history-timeplusd-0 -n $(NS)
	-kubectl delete pvc timeplusd-history-timeplusd-1 -n $(NS)
	-kubectl delete pvc timeplusd-history-timeplusd-2 -n $(NS)
	-kubectl delete pvc timeplusd-stream-timeplusd-0   -n $(NS)
	-kubectl delete pvc timeplusd-stream-timeplusd-1   -n $(NS)
	-kubectl delete pvc timeplusd-stream-timeplusd-2   -n $(NS)
	-kubectl delete pvc timeplusd-log-timeplusd-0 -n $(NS)
	-kubectl delete pvc timeplusd-log-timeplusd-1 -n $(NS)
	-kubectl delete pvc timeplusd-log-timeplusd-2 -n $(NS)

forwards:
	kubectl port-forward svc/timeplus-cli 8000:8000 -n $(NS) --address 0.0.0.0

expose:
	kubectl expose deployment timeplus-appserver -n $(NS) --type=LoadBalancer --name=timeplus-appserver-elb --port=8000 --target-port=8000

ano:
	kubectl annotate service timeplus-appserver-elb service.beta.kubernetes.io/aws-load-balancer-type="external" -n user-gang
	kubectl annotate service timeplus-appserver-elb service.beta.kubernetes.io/aws-load-balancer-nlb-target-type="ip" -n user-gang
	kubectl annotate service timeplus-appserver-elb service.beta.kubernetes.io/aws-load-balancer-scheme="internet-facing" -n user-gang

upgrade:
	kubectl scale statefulset timeplusd --replicas=0 -n $(NS)
	helm -n $(NS) upgrade $(RELEASE) ./timeplus-enterprise-v${VERSION}.tgz \
		-f ./values_$(PLATFORM).yaml
	kubectl scale statefulset timeplusd --replicas=1 -n $(NS)

logs:
	kubectl cp timeplusd-0:/var/log/timeplusd-server/timeplusd-server.log ./logs/timeplusd-server-0.log -c timeplusd -n $(NS)
	kubectl cp timeplusd-1:/var/log/timeplusd-server/timeplusd-server.log ./logs/timeplusd-server-1.log -c timeplusd -n $(NS)
	kubectl cp timeplusd-2:/var/log/timeplusd-server/timeplusd-server.log ./logs/timeplusd-server-2.log -c timeplusd -n $(NS)

forward-timeplusd:
	kubectl port-forward svc/timeplusd-svc 8463:8463 -n $(NS)

sync:
	./timeplus sync apply ./scripts

		 
