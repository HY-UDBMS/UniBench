# UniBench - Towards Benchmarking the Multi-Model DBMS
The Unibench project aims to develop a generic benchmark for a holistic evaluation of multi-model database systems (MMDS), which are able to support multiple data models such as documents, graph, and key-value models in a single back-end. UniBench consists of a set of mixed data models that mimics a social commerce application, which covers data models including JSON, XML, key-value, tabular, and graph. The UniBench workload consists of a set of complex read-only queries and read-write transactions that involve at least two data models.

Please access [the TPCTC 2018 paper](https://www.cs.helsinki.fi/u/jilu/documents/UniBench.pdf) to find more details:

```
Zhang, Chao, et al. "UniBench: A benchmark for multi-model database management systems." TPCTC. Springer, Cham, 2018.
```

## Environment

To run this benchmark, you need to have the systems under test installed, and JRE >=8 installed. The current implementations include four state-of-the-art MMDB. Namely, ArangoDB (Query Language: AQL), OrientDB (Query Language: Orient SQL), AgensGraph (Query Language: SQL/Cypher), and Spark (partial evaluation using Spark SQL), you may also employ Unibench to evaluate a new MMDB by the following steps: (1) write an importing script or program, (2) <em>extends</em> the MMDB abstract class. (3) implement the <em>connection</em> and <em>query</em> methods in the corresponding MMDB class.

## Running

Note that ArangoDB uses the default <em>_System</em> database and the password is empty. OrientDB uses a database named <em>test</em> that you may create beforehand. Download the release as follow:
https://github.com/HY-UDBMS/Unibench/releases

```
./DataImporting_ArangoDB.sh
java -jar Unibench.jar ArangoDB Q1
```
