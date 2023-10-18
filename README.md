# DBKnitter

Knitting DBs...

## Experiment

Prerequisites
- TPC-H data at directory `./tpch`
  - The following example have TPC-H data with scale 0.1 at `./tpch/s01`

Start up a cluster of platforms.
```bash
bash cloudlab/start-all.sh
```

Loading TPC-H tables splitted among platforms.
```bash
# ... <TPC-H data directory> <MySQL tables> <MongoDB tables>
bash cloudlab/tpch_init.sh /tpch/s01 nation,lineitem,part region,supplier,partsupp,customer,orders
```

Clearing the cluster of platforms, including database data.
```bash
bash cloudlab/clear-all.sh
```
