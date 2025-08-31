NS=kafka

expose:
	kubectl expose deployment redpanda-console  -n $(NS) --type=LoadBalancer --name=redpanda-console-elb --port=8080 --target-port=8080
