# HIGUN - Hyper Indexer of General UTXO Network

HIGUN is the abbreviation for **Hyper Indexer of General UTXO Network**. As a Bitcoin sidechain, MVC has been committed to expanding Bitcoin's performance. However, there has been a lack of a high-performance UTXO indexer that integrates Bitcoin and other sidechains in the market. 

This high-performance universal UTXO network indexer, jointly developed by the MVC technical team and Metalet team, will for the first time index both Bitcoin and MVC UTXOs simultaneously in a single indexer, treating these two as an integrated network. It adopts Pebble files instead of traditional databases, and a medium-configuration server can actually process tens of millions of transactions daily in performance tests.

## Current Features (v1.0)

- **Bitcoin UTXO Indexing**
- **MVC UTXO Indexing** 
- **MVC FT Indexing**
- **Block Information Indexing**

## Future Plans

Future versions will add support for more asset protocols and more Bitcoin sidechains.

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
