
.PHONY: install

# timeplusd python --config-file /etc/timeplusd-server/config.yaml -m pip install --user faker
install_dep:
	curl -X POST http://localhost:8123/timeplusd/v1/python_packages \
		-u admin:Password! \
		-d '{"packages": [{"name": "faker"}]}'

	curl -H "x-timeplus-user: admin" -H "x-timeplus-key:Password!" -X POST http://localhost:8123/timeplusd/v1/python_packages \
		-d '{"packages": [{"name": "faker"}]}'
	
