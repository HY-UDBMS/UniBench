import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.orientechnologies.orient.jdbc.OrientJdbcConnection;

import net.bitnine.agensgraph.Driver;
import net.bitnine.agensgraph.graph.Vertex;

public class AgensGraph extends MMDB {
	Object Connection(){
		Connection conn=null;
		try {
			Class.forName("net.bitnine.agensgraph.Driver");
			String connectionString = "jdbc:agensgraph://url:5432/test";
			String username = "username";
			String password = "password";
			conn = DriverManager.getConnection(connectionString, username, password);
			Statement stmt_path = conn.createStatement();
			stmt_path.execute("set graph_path = social_network");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
        
        return (Object)conn;
	}
	
    void Q1(String PersonId) {
    	String Q1="SELECT COUNT(*) from customer_profile c, orders o, feedback f,"
    			+ 	"(MATCH (c:Customers {id:?} )-[:personhascreated]->(p:posts) return p) as r "
    			+ "WHERE c.id=? and o.data->>'PersonId'= ? and f.personid= ?";
    	Connection conn = (Connection) this.Connection();
		try {
			PreparedStatement stmt1 = conn.prepareStatement(Q1);				
			stmt1.setString(1, PersonId);
			stmt1.setString(2, PersonId);				
			stmt1.setString(3, PersonId);	
			stmt1.setString(4, PersonId);	
			long millisStart1 = System.currentTimeMillis();
			ResultSet rs1 = stmt1.executeQuery();
			long millisEnd1 = System.currentTimeMillis();
			System.out.println("Query 1 took "+(millisEnd1 - millisStart1) + " ms");
		} catch (SQLException e) {
			e.printStackTrace();
		}
    } 
    
    void Q2(String ProductId) {
    	String Q2="SELECT f.feedback, p->>'content' "
    			+ "FROM feedback as f ,(MATCH (p:posts)-[:posthastag]->(t:tags {asin:?}) return p,t) as p "
    			+ "WHERE f.asin=? and cast(substring(f.feedback from 2 for 2) as double precision)<5";
    	Connection conn = (Connection) this.Connection();
	    try {
			long millisStart2 = System.currentTimeMillis();
			PreparedStatement stmt2 = conn.prepareStatement(Q2);
			stmt2.setString(1, ProductId);
			stmt2.setString(2, ProductId);
			ResultSet rs2 = stmt2.executeQuery();
			long millisEnd2 = System.currentTimeMillis();
			System.out.println("Query 2 took "+(millisEnd2 - millisStart2) + " ms");
		} catch (SQLException e) {
			e.printStackTrace();
		}
    } 
    
    void Q3(String ProductId) {
    	String Q3="SELECT count(distinct(c)) "
    			+ "FROM orders o "
    			+ "CROSS JOIN jsonb_array_elements(o.data->'Orderline') ol "
    			+ "INNER JOIN (MATCH (c:Customers)-[:personhascreated]->(p:posts)-[:posthastag]->(:tags {productid:?}) "
    			+ "RETURN distinct(c)) as c3 ON o.data->'PersonId'=c->'id' "
    			+ "WHERE  ol->>'productId'=?";
    	Connection conn = (Connection) this.Connection();
	    try {
			long millisStart3 = System.currentTimeMillis();
			PreparedStatement stmt3 = conn.prepareStatement(Q3);
			stmt3.setString(1, ProductId);
			stmt3.setString(2, ProductId);
			ResultSet rs3 = stmt3.executeQuery();
			long millisEnd3 = System.currentTimeMillis();
			System.out.println("Query 3 took "+(millisEnd3 - millisStart3) + " ms");
		} catch (SQLException e) {
			e.printStackTrace();
		}
    } 
    
    void Q4() {
    	String Q4="WITH top2 as (select array_agg(rich) as persons  from (Select  o.data->>'PersonId' as rich, sum((o.data->>'TotalPrice'):: double precision) as sum from orders o group by o.data->>'PersonId' order by sum desc limit 2) as rich)"
    			+  " SELECT commonfriends from "
    			+ "		(MATCH (first:customers)-[:knows*3]-> (commonfriends:customers) <-[:knows]-(second:customers) "
    			+ 		"WHERE first.id=(select persons[1] from top2) and second.id=(select persons[2] from top2) "
    			+ 		"RETURN commonfriends) as m";
    	Connection conn = (Connection) this.Connection();
	    try {
			long millisStart4 = System.currentTimeMillis();
			PreparedStatement stmt4 = conn.prepareStatement(Q4);
			ResultSet rs4 = stmt4.executeQuery();
			long millisEnd4 = System.currentTimeMillis();
			System.out.println("Query 4 took "+(millisEnd4 - millisStart4) + " ms");
		} catch (SQLException e) {
			e.printStackTrace();
		}
    } 
    
    void Q5(String PersonId, String brand) {
    	String Q5="MATCH (c:customers)-[:personhascreated]->(p:posts) "
    			+ "Where (c->'id' in (SELECT distinct(c2)->'id' "
    			+ "FROM orders CROSS JOIN jsonb_array_elements(orders.data->'Orderline') ol "
    			+ "INNER JOIN (MATCH (c1:Customers {id:?})-[:knows]->(c2:Customers) return c2) as c3 "
    			+ "ON orders.data->'PersonId'=c2->'id' "
    			+ "WHERE ol->>'brand'=?)) Return c, p";
    	Connection conn = (Connection) this.Connection();
	    try {
			long millisStart5 = System.currentTimeMillis();
			PreparedStatement stmt5 = conn.prepareStatement(Q5);
			String Q5PersonId=PersonId;
			String Q5Brand=brand;
			stmt5.setString(1, Q5PersonId);
			stmt5.setString(2, Q5Brand);
			ResultSet rs5 = stmt5.executeQuery();
			long millisEnd5 = System.currentTimeMillis();
			System.out.println("Query 5 took "+(millisEnd5 - millisStart5) + " ms");
		} catch (SQLException e) {
			e.printStackTrace();
		}
    } 
    
    void Q6(String startPerson, String EndPerson) {
    	String Q6="SELECT ol->'asin',count(*) as sales  "
    			+ 	"FROM (select jsonb_array_elements(o.data->'Orderline') as ol "
    			+ 	"FROM orders o "
    			+ 	"JOIN (select jsonb_array_elements(nodes)->'properties'->'id' as ids "
    			+ 	"FROM (MATCH p = shortestPath((s:customers {id:?})-[:knows*]-(t:customers {id:?})) "
    			+ 	"RETURN to_jsonb(nodes(p)) as nodes) as nodes) as id on o.data->'PersonId'=ids) as ol "
    			+ "GROUP BY ol->'asin' order by sales DESC LIMIT 3";
    	Connection conn = (Connection) this.Connection();
	    try {
			long millisStart6 = System.currentTimeMillis();
			PreparedStatement stmt6 = conn.prepareStatement(Q6);
			String Q6PersonId1=startPerson;
			String Q6PersonId2=EndPerson;
			stmt6.setString(1, Q6PersonId1);
			stmt6.setString(2, Q6PersonId2);
			ResultSet rs6 = stmt6.executeQuery();
			long millisEnd6 = System.currentTimeMillis();
			System.out.println("Query 6 took "+(millisEnd6 - millisStart6) + " ms");
		} catch (SQLException e) {
			e.printStackTrace();
		}
    } 
    
    void Q7(String brand) { 
    	String Q7="WITH sales1 as (SELECT ol->>'asin' as asin, count(ol) "
    			+ "FROM orders o cross join jsonb_array_elements(o.data->'Orderline') ol where  ol->>'brand'=? and  EXTRACT(YEAR from to_date(o.data->>'OrderDate','YYYY'))= 2019 Group by ol->>'asin' Order by count DESC limit 10), "
    			+ 	"sales2 as (SELECT ol->>'asin' as asin, count(ol) FROM orders o cross join jsonb_array_elements(o.data->'Orderline') ol where ol->>'brand'='Anta_Sports' and  EXTRACT(YEAR from to_date(o.data->>'OrderDate','YYYY'))= 2020 Group by ol->>'asin' Order by count DESC limit 10) "
    			+ "SELECT distinct(sales1.asin), (sales1.count-sales2.count) as residual, Feedback.feedback from sales1 "
    			+ "INNER JOIN sales2 on sales1.asin=sales2.asin "
    			+ "INNER JOIN Feedback on sales1.asin=Feedback.asin  "
    			+ "WHERE sales1.count>sales2.count";
    	Connection conn = (Connection) this.Connection();
	    try {
			long millisStart7 = System.currentTimeMillis();
			PreparedStatement stmt7 = conn.prepareStatement(Q7);
			String Q7Brand=brand;
			stmt7.setString(1, Q7Brand);
			ResultSet rs7 = stmt7.executeQuery();
			long millisEnd7 = System.currentTimeMillis();
			System.out.println("Query 7 took "+(millisEnd7 - millisStart7) + " ms");
		} catch (SQLException e) {
			e.printStackTrace();
		}
    } 
    
    void Q8() { 
    	String Q8="WITH brandList as (select name from vendor where country='China'), "
    			+ 	"Totalsales as (SELECT ol->>'asin'as asin,count(ol) FROM orders o, jsonb_array_elements(o.data->'Orderline') ol inner join brandList bl on bl.name=ol->>'brand' Group by ol->>'asin' Order by count DESC) "
    			+ "SELECT s.asin,count(asin) from Totalsales s "
    			+ "INNER JOIN (MATCH (p:posts)-[:posthastag]->(t:tags) return t) as p on s.asin=t->>'asin' "
    			+ "GROUP BY s.asin Order by count DESC";
    	Connection conn = (Connection) this.Connection();
	    try {
			long millisStart8 = System.currentTimeMillis();
			PreparedStatement stmt8 = conn.prepareStatement(Q8);
			ResultSet rs8 = stmt8.executeQuery();
			long millisEnd8 = System.currentTimeMillis();
			System.out.println("Query 8 took "+(millisEnd8 - millisStart8) + " ms");
		} catch (SQLException e) {
			e.printStackTrace();
		}
    } 
    
    void Q9() {
    	String Q9=" WITH brandList as (select name from vendor where country='China'), "
    			+ 	"TopCompany as (SELECT ol->>'brand'as company, c.gender, count(*) FROM orders o "
    			+ 	"CROSS JOIN jsonb_array_elements(o.data->'Orderline') ol "
    			+ 	"INNER JOIN brandList bl on bl.name=ol->>'brand' "
    			+ 	"INNER JOIN customer_profile c on o.data->>'PersonId'=c.id "
    			+ 	"GROUP BY company, c.gender Order by count DESC, company DESC  limit 6) "
    			+ "SELECT tc, bl.name,r FROM  orders o  "
    			+ "CROSS JOIN TopCompany tc  "
    			+ "CROSS JOIN jsonb_array_elements(o.data->'Orderline') ol "
    			+ "INNER JOIN brandList bl on bl.name=ol->>'brand' "
    			+ "INNER JOIN  (MATCH (p:posts)-[:posthastag]->(t:tags) return t,p) as r on ol->>'productId'=t->>'productid' "
    			+ "WHERE EXTRACT(YEAR from to_date(r.p->>'creationdate','YYYY'))= 2011 limit 10";
    	Connection conn = (Connection) this.Connection();
	    try {
			long millisStart9 = System.currentTimeMillis();
			PreparedStatement stmt9 = conn.prepareStatement(Q9);
			ResultSet rs9 = stmt9.executeQuery();
			long millisEnd9 = System.currentTimeMillis();
			System.out.println("Query 9 took "+(millisEnd9 - millisStart9) + " ms");
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }
    
    void Q10() {
    	String Q10="SELECT o.data->'PersonId' as Active_person, max(o.data->>'OrderDate') as Recency, count(o.data) as Frequency, sum((o.data->>'TotalPrice'):: double precision) as Monetary  "
				+ "FROM orders o "
				+ "INNER JOIN (MATCH (c:customers)-[:personhascreated]->(p:posts) "
				+ "WHERE p->>'creationdate'>'2012-10'  "
				+ "RETURN c->'id' as pid, count(p) order by count DESC limit 10) as id on o.data->'PersonId'=pid "
				+ "GROUP BY o.data->'PersonId'";
    	Connection conn = (Connection) this.Connection();
	    try {
			long millisStart10 = System.currentTimeMillis();
			PreparedStatement stmt10 = conn.prepareStatement(Q10);
			ResultSet rs10 = stmt10.executeQuery();
			long millisEnd10 = System.currentTimeMillis();
			System.out.println("Query 10 took "+(millisEnd10 - millisStart10) + " ms");
		} catch (SQLException e) {
			e.printStackTrace();
		}
    } 
}
