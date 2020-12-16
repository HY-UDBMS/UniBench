# UniBench - Towards Benchmarking the Multi-Model DBMS
The [UniBench project](https://www.helsinki.fi/en/researchgroups/unified-database-management-systems-udbms/unibench-towards-benchmarking-multi-model-dbms) aims to develop a generic benchmark for a holistic evaluation of multi-model database systems (MMDS), which are able to support multiple data models such as documents, graph, and key-value models in a single back-end. UniBench consists of a set of mixed data models that mimics a social commerce application, which covers data models including JSON, XML, key-value, tabular, and graph. The UniBench workload consists of a set of complex read-only queries and read-write transactions that involve at least two data models.

Please access our [DPD 2019 paper](http://link.springer.com/article/10.1007/s10619-019-07279-6) and [TPCTC 2018 paper](https://www.cs.helsinki.fi/u/jilu/documents/UniBench.pdf) to find more details:

```
Zhang, Chao, et al. "Holistic Evaluation in Multi-Model Databases Benchmarking." In Distributed and Parallel Databases, 2019.

Zhang, Chao, et al. "UniBench: A benchmark for multi-model database management systems." TPCTC. Springer, Cham, 2018.
```

## Environment

To run this benchmark, you need to have the systems under test installed, and JRE >=8 installed. The current implementations include four state-of-the-art MMDB. Namely, ArangoDB (Query Language: AQL), OrientDB (Query Language: Orient SQL), AgensGraph (Query Language: SQL/Cypher), and Spark (partial evaluation using Spark SQL), you may also employ Unibench to evaluate a new MMDB by the following steps: (1) write an importing script or program, (2) <em>extends</em> the MMDB abstract class. (3) implement the <em>connection</em> and <em>query</em> methods in the corresponding MMDB class.

## Running

Note that ArangoDB uses the default <em>_System</em> database and the password is empty. OrientDB uses a database named <em>test</em> that you may create beforehand. Download the latest release with SF1 dataset at
https://github.com/HY-UDBMS/UniBench/releases/tag/0.2, and try out the first multi-model query as follows:

```
./DataImporting_ArangoDB.sh
java -jar Unibench.jar ArangoDB Q1
```

## Notes

(1) larger datasets with SF10 and SF30 can be found at https://github.com/HY-UDBMS/UniBench/releases/tag/data.

(2) for the UniBench schema in the DPD paper, the tag table and the product table use the same data of Product.csv, the productId has the one-to-one mapping to the tagid. The hasInterest relation is removed since the queries do not involve it.

(3) for the data importing, we have released the scripts for ArangoDB and OrientDB based on their importer, since they have evolved several versions, please check if some parameters need to be changed. For example, ArangoDB 3.7 has used arangoimport utility to replace arangoimp, and the authentication needs to be turned off.

(4) if the benchmark can not find the parameter files (Brands, PersonIds, ProductIds), please download them from the github repo to the directory with path ./UniBench/Unibench/ 

(5) To run the benchmark in OrientDB, you need to create a custom javascript function named <em>compareList</em> in the Functions panel of OrientDB as follow: 

```
var IDs = new Array();

  for(var i=0;i<array1.length;i++){
      for(var j=0;j<array2.length;j++){
        if( (array1[i].field('asin')==array2[j].field('asin')) && (array1[i].field('cnt')>array2[j].field('cnt')) )
				IDs.push(array1[i]);
     }
  }

return IDs;
```

Importing the data to OrientDB by a single command under the DataImporting_OrientDB folder as follow:  
```
./Orientimport.sh
```

## Data provenance
We collect the metadata from three datasets, namely, [LDBC dataset](https://github.com/ldbc/ldbc_snb_datagen), [DBpedia dataset](https://wiki.dbpedia.org/), and [Amazon product dataset](http://jmcauley.ucsd.edu/data/amazon/), and we use these data for purposes of academic research.
