## RUN DOCKER

```
docker build -t utxo_indexer .

docker run -d \
  --name mvc-utxo  --network=host  --restart=always -m 10g \
  -v /your/path/utxo_indexer:/app/utxo_indexer \
  -v /your/path/config.yaml:/app/config.yaml \
  -v /your/path/data:/app/data \
  -v /your/path/blockinfo_data:/app/blockinfo_data \
  utxo_indexer
```
