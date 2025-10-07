NS=data
NAME=cardemo

POD=$(shell kubectl get pods --selector=app=$(NAME) -n $(NS) -o jsonpath="{.items[0].metadata.name}")


config: delete_config
        kubectl create configmap $(NAME)-config --from-file=./configmap/ -n $(NS)

delete_config:
        kubectl delete configmap $(NAME)-config --ignore-not-found=true -n $(NS)

install:
        kubectl apply -f ./deployment.yaml

uninstall:
        kubectl delete deployment $(NAME) -n $(NS)


logs:
        kubectl logs $(POD) -n $(NS) -f