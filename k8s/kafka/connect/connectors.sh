curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql-retailer-cdc-connector",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
      "database.hostname": "35.247.93.97",
      "database.port": "3306",
      "database.user": "admin",
      "database.password": "Password!",
      "database.server.id": "12345",
      "topic.prefix": "demo.cdc.mysql.retailer",
      "database.include.list": "retailer",
      "table.include.list": "retailer.orders,retailer.orderdetails,retailer.products",
      "transforms": "route",
      "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
      "transforms.route.regex": "demo.cdc.mysql.retailer.retailer.(.*)",
      "transforms.route.replacement": "demo.cdc.mysql.retailer.$1",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false"
    }
  }'