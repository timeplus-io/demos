
nats stream add MARKET_DATA \
  -s nats.nats:4222 \
  --subjects "market_data" \
  --retention limits \
  --storage file \
  --replicas 1 \
  --defaults

nats stream add TRANSACTIONS \
  -s nats.nats:4222 \
  --subjects "transactions" \
  --retention limits \
  --storage file \
  --replicas 1 \
  --defaults