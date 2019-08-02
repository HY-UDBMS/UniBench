# Unibench - Towards Benchmarking the Multi-Model Database Systems
The Unibench project aims to develop a generic benchmark for a holistic evaluation of multi-model database systems (MMDS), which are able to support multiple data models such as documents, graph, and key-value models in a single back-end. UniBench consists of a set of mixed data models that mimics a social commerce application, which covers data models including JSON, XML, key-value, tabular, and graph. The UniBench workload consists of a set of complex read-only queries and read-write transactions that involve at least two data models.

Please access [the TPCTC 2018 paper](https://www.cs.helsinki.fi/u/jilu/documents/UniBench.pdf) to find more details:

The current implementations include four the-state-of-the-art MMDS, ArangoDB, OrientDB, AgensGraph, and Spark, user can also develop new query of interest to gain the sights.     


```
Zhang, Chao, et al. "UniBench: A benchmark for multi-model database management systems." TPCTC. Springer, Cham, 2018.
