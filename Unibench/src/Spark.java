/*
 * build.sbt
 * 
	name := "UniBench_Spark"

	version := "0.1"

	scalaVersion := "2.11.6"

	resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"

	libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
	libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
	libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.3"
	libraryDependencies += "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"
 * 
 * 
 * 
 
import org.apache.spark.{SparkConf, SparkContext}
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.array*/


 //Querys Q1,Q5, and Q8 are available now
public class Spark extends MMDB {
	Object Connection(){
/*		 val conf = new SparkConf()
				    conf.setMaster("local")
				    conf.setAppName("Graph Query")
				    val sc = new SparkContext(conf)
				    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		return conf;*/
		return null;
	}
	
    void Q1(String PersonId) {
/*      var r1 = sqlContext.read.format("csv").option("header", "true").option("delimiter","|").load("HDFS://person_0_0.csv")
      r1.toDF().filter("id='4145'").show()

      var r2 = sqlContext.read.format("csv").option("header", "false").option("delimiter","|").load("HDFS://Feedback.csv")
      r2.toDF("asin","PersonId","feedback").filter("PersonId='4145'").show()

      val j = sqlContext.read.format("json").option("header", "true").option("delimiter","|").load("HDFS://Order.json")
      j.toDF().filter("PersonId='4145'").show()*/
    } 
    
    void Q2(String ProductId) { System.out.println("Not yet"); } 
    
    void Q3(String ProductId) { System.out.println("Not yet"); } 
    
    void Q4() { System.out.println("Not yet"); } 
    
    void Q5(String PersonId, String brand) {
/*        val persons   = sqlContext.read.format("csv").load("HDFS://person.csv").toDF()
        val orders    = sqlContext.read.format("json").load("HDFS://order.json").toDF()                    
        val knows     = sqlContext.read.format("csv").load("HDFS://knows.csv").toDF("src", "dst")
        val graph     = GraphFrame(persons, knows)
        val friends   = graph.find("(a)-[e1]->(b);(b)-[e2]->(c)").filter("a.id=56").select(explode(array("a.id", "b.id")).alias("PersonId")).distinct
        val orders	  = orders.where(array_contains(col("items.brand"),"Nike"))
        val result 	  = orders.join(friends,Seq("PersonId"),"@inner@").select("PersonId","items").collect()*/
    } 
    
    void Q6(String startPerson, String EndPerson) { System.out.println("Not yet"); } 
    
    void Q7(String brand) { System.out.println("Not yet"); } 
    
    void Q8() {
/*        val j = sqlContext.read.format("json").option("header", "true").option("delimiter","|").load("HDFS://Order.json")
        val OrdersWithBrand=j.toDF().withColumn("line", explode($"Orderline")).drop("Orderline").select("line.productId").groupBy("productId").count().sort(desc("count")).limit(10)
        val PostHasTag = sqlContext.read.format("csv").option("header", "true").option("delimiter","|").load("HDFS://post_hasTag_tag_0_0.csv").toDF("PostId","productId")
        val top_tags = PostHasTag.groupBy("productId").count().sort(desc("count"))
        OrdersWithBrand.join(top_tags,Seq("productId"),"inner").collect()*/
    } 
    
    void Q9() { System.out.println("Not yet"); }
    
    void Q10() { System.out.println("Not yet"); } 
}
