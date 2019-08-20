import java.io.IOException;
import java.util.Map;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.util.MapBuilder;
import com.arangodb.velocypack.VPackSlice;
import com.arangodb.velocypack.exception.VPackException;

public class Arango extends MMDB {
	
		Object Connection(){
			ArangoDB arangoDB = new ArangoDB.Builder()
					.host("localhost", 8529).user("root").password("123")
					.build();
			
			return (Object)arangoDB;
		}
		
	    void Q1(String PersonId) { 
	    		String AQ1 = "LET customer=(FOR doc IN Customer FILTER doc._key==@key RETURN doc ) "
				+ "LET orders=(For order in Order Filter order.PersonId==@key return order) "
				+ "LET feedback=(For feedback in Feedback Filter TO_STRING(feedback.PersonId)==@key return feedback) "
				+ "LET posts=(For post in Outbound @id PersonHasPost return post) LET plist=Flatten(For order in orders return order.Orderline[*]) "
				+ "LET list1=(For item in plist  collect category=item.brand with count into cnt sort cnt DESC return {category,cnt}) "
				+ "LET list2=(For post in posts For Tag in Outbound post PostHasTag Collect id= Tag._key WITH COUNT INTO cnt SORT cnt DESC Return {id,cnt}) "
				+ "Return {customer,orders,feedback,posts,list1,list2}";	
	    		
	    		ArangoDB Conn=(ArangoDB)this.Connection();
	    		
	    		long millisStart1 = System.currentTimeMillis();
	    		Map<String, Object> bindVars1 = new MapBuilder()
	    				.put("key", PersonId)
	    				.put("id", "Customer/"+PersonId)
	    				.get();
	    		ArangoCursor<String> cursor1 = Conn.db("_system").query(AQ1,bindVars1,String.class);
	    	    long millisEnd1 = System.currentTimeMillis();
	    	    System.out.println("Query 1 took "+(millisEnd1 - millisStart1) + " ms");
	    	        	   
	    		try {
					cursor1.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		 } 
	    
	    void Q2(String ProductId) { 			
	    	
	    	String AQ2 = "Let personlist =(For post in Inbound @id PostHasTag For person in Inbound post._id PersonHasTag limit 100 return person._key) "
				+ "For order in Order Filter order.OrderDate>\"2022\" and @key in order.Orderline[*].productId and order.PersonId in Unique(personlist) "
				+ "Return Distinct(order.PersonId)";		

	    	ArangoDB Conn=(ArangoDB)this.Connection();
	    	long millisStart2 = System.currentTimeMillis();
	    	Map<String, Object> bindVars2 = new MapBuilder()
	    			.put("key", ProductId)
	    			.put("id", "Product/"+ProductId)
	    			.get();
	    	ArangoCursor<String> cursor2 = Conn.db("_system").query(AQ2,bindVars2,String.class);
	    	long millisEnd2 = System.currentTimeMillis();
	    	System.out.println("Query 2 took "+(millisEnd2 - millisStart2) + " ms"); 
	    	} 
	    
	    void Q3(String ProductId) {
		    String AQ3="Let posts=(For post in Inbound @id PostHasTag  return post) "
		    		+ "Let feedback=(For feedback in Feedback Filter feedback.asin==@id and TO_NUMBER(SUBSTRING(feedback.feedback,1,1)) < 5 return feedback) "
		    		+ "Return {posts,feedback}";
		    
		    ArangoDB Conn=(ArangoDB)this.Connection();
			long millisStart3 = System.currentTimeMillis();
			Map<String, Object> bindVars3 = new MapBuilder()
					.put("id", "Product/"+ProductId)
					.get();
			ArangoCursor<String> cursor3 = Conn.db("_system").query(AQ3,bindVars3,String.class);
		    long millisEnd3 = System.currentTimeMillis();
		    System.out.println("Query 3 took "+(millisEnd3 - millisStart3) + " ms");
	    } 
	    
	    void Q4() {
		    String AQ4="let plist=(For order in Order collect PersonId=order.PersonId into group sort SUM(group[*].order.TotalPrice) DESC LIMIT 2 return {PersonId,Monetary:SUM(group[*].order.TotalPrice)}) "
		    		+ "let set1=( For vertex in 1..3 outbound CONCAT(\"Customer/\",plist[0].PersonId) KnowsGraph return vertex) "
		    				+ "let set2=( For vertex in 1..3 outbound CONCAT(\"Customer/\",plist[1].PersonId) KnowsGraph return vertex) "
		    						+ "Return count(intersection(set1,set2))";
		    ArangoDB Conn=(ArangoDB)this.Connection();
			long millisStart4 = System.currentTimeMillis();
			ArangoCursor<String> cursor4 = Conn.db("_system").query(AQ4,String.class);
		    long millisEnd4 = System.currentTimeMillis();
		    System.out.println("Query 4 took "+(millisEnd4 - millisStart4) + " ms");
	    } 
	    
	    void Q5(String PersonId, String brand) {
		    String AQ5="Let Plist=(For friend in 1..1 Outbound @id KnowsGraph "
		    		+ "For order in Order Filter order.PersonId==friend._key and @brand in order.Orderline[*].brand return distinct(friend)) "
		    		+ "For person in Plist For post in Outbound person._id PersonHasTag "
		    		+ "For tag in Outbound post PostHasTag Return {person:person,tags:tag}";
		    
		    ArangoDB Conn=(ArangoDB)this.Connection();
			long millisStart5 = System.currentTimeMillis();
			Map<String, Object> bindVars5 = new MapBuilder()
					.put("id", "Customer/"+PersonId)
					.put("brand", brand)
					.get();
			ArangoCursor<String> cursor5 = Conn.db("_system").query(AQ5,bindVars5,String.class);
		    long millisEnd5 = System.currentTimeMillis();
		    System.out.println("Query 5 took "+(millisEnd5 - millisStart5) + " ms");
	    } 
	    
	    void Q6(String src,String dst) {
		    String AQ6="LET shortestPath= (FOR vertex, edge IN OUTBOUND SHORTEST_PATH @customerOne TO @customerTwo KnowsGraph  Return vertex) "
		    		+ "LET plist = Flatten( For item in shortestPath For order in Order  Filter item._key==order.PersonId Return order.Orderline) "
		    		+ "For item in plist collect productId=item.productId with count into cnt Sort cnt desc limit 5 "
		    		+ "Return {productId,cnt}";
		    ArangoDB Conn=(ArangoDB)this.Connection();
			long millisStart6 = System.currentTimeMillis();
			Map<String, Object> bindVars6 = new MapBuilder()
					.put("customerOne", "Customer/"+src)
					.put("customerTwo", "Customer/"+dst)
					.get();
			ArangoCursor<String> cursor6 = Conn.db("_system").query(AQ6,bindVars6,String.class);
		    long millisEnd6 = System.currentTimeMillis();
		    System.out.println("Query 6 took "+(millisEnd6 - millisStart6) + " ms");
	    } 
	    
	    void Q7(String brand) {
		    String AQ7="LET productList1=Flatten(For order in Order Filter order.OrderDate < \"2019\" and order.OrderDate > \"2018\"  and @brand in order.Orderline[*].brand Return order.Orderline) "
		    		+ "LET sales1=(For item in productList1 Filter item.brand==@brand collect asin=item.asin with count into cnt Sort cnt desc return {asin,cnt}) "
		    		+ "LET productList2= Flatten(For order in Order Filter order.OrderDate < \"2020\" and order.OrderDate > \"2019\" and  @brand in order.Orderline[*].brand Return order.Orderline) "
		    		+ "LET sales2=(For item in productList2 Filter item.brand==@brand collect asin=item.asin with count into cnt Sort cnt desc return {asin,cnt}) "
		    		+ "LET declineList=( For item1 in sales1 For item2 in sales2 Filter item1.asin==item2.asin and item1.cnt > item2.cnt return item1.asin ) "
		    		+ "For item in declineList For feedback in Feedback Filter item==feedback.asin return feedback";
		    ArangoDB Conn=(ArangoDB)this.Connection();
			long millisStart7 = System.currentTimeMillis();
			Map<String, Object> bindVars7 = new MapBuilder()
					.put("brand", brand)
					.get();
			ArangoCursor<String> cursor7 = Conn.db("_system").query(AQ7,bindVars7,String.class);
		    long millisEnd7 = System.currentTimeMillis();
		    System.out.println("Query 7 took "+(millisEnd7 - millisStart7) + " ms");
	    } 
	    
	    void Q8() {
		    String AQ8="LET brands=(For brand in Vendor Filter brand.country==\"China\" return brand.name) "
		    		+ "LET orderlines=Flatten(For order in Order Filter order.OrderDate < \"2019\" and order.OrderDate > \"2018\" and count(intersection(brands,order.Orderline[*].brand))>0 return order.Orderline) "
		    				+ "LET lines=(For line in orderlines Filter line.brand in brands return line) "
		    				+ "LET popularity=Count(For item in Unique(lines) "
		    				+ "For post in Inbound CONCAT(\"Product/\",item.productId) PostHasTag Return post)  Return popularity";
		    ArangoDB Conn=(ArangoDB)this.Connection();
			long millisStart8 = System.currentTimeMillis();
			ArangoCursor<String> cursor8 = Conn.db("_system").query(AQ8,String.class);
		    long millisEnd8 = System.currentTimeMillis();
		    System.out.println("Query 8 took "+(millisEnd8 - millisStart8) + " ms");
	    } 
	    
	    void Q9() {
		    String AQ9="LET vendors=(For brand in Vendor filter brand.country==\"China\" return brand.name) "
		    		+ "LET brands=FLATTEN(For order in Order Filter order.OrderDate < \"2019\" and order.OrderDate > \"2018\" and count(intersection(vendors,order.Orderline[*].brand))>0 Return (order.Orderline[*].brand))[**] "
		    				+ "LET top_companies=(For brand in brands Filter brand in vendors Collect name=brand with count into sales Sort sales DESC LIMIT 3 Return {name,sales}) "
		    				+ "LET merged=( For company in top_companies For order in Order Filter order.OrderDate < \"2019\" and order.OrderDate > \"2018\" and company.name in order.Orderline[*].brand For customer in Customer Filter customer._key==order.PersonId Return {name:company.name,persons:order.PersonId,gender:customer.gender}) "
		    				+ "LET groups= (For item in merged Collect name=item.name, gender=item.gender into sales Return {name:name,gender:gender,sales:count(sales), plist:sales[*].item.persons}) "
		    				+ "For group in groups For person in group.plist For post in Outbound Concat('Customer/',person) PersonHasTag Filter post.creationDate>\"2012-10-10\" Collect entry=group with count into posts Sort posts DESC Return {entry:entry,posts:posts}";
		    ArangoDB Conn=(ArangoDB)this.Connection();
			long millisStart9 = System.currentTimeMillis();
			ArangoCursor<String> cursor9 = Conn.db("_system").query(AQ9,String.class);
		    long millisEnd9 = System.currentTimeMillis();
		    System.out.println("Query 9 took "+(millisEnd9 - millisStart9) + " ms");
	    }
	    
	    void Q10() {
		    String AQ10="LET personList=(FOR post IN Post Filter post.creationDate>\"2012-10\" FOR person IN INBOUND post PersonHasTag COLLECT activist=person WITH COUNT INTO cnt SORT cnt DESC LIMIT 10 RETURN activist._key) FOR order IN Order FILTER order.PersonId IN personList COLLECT c=order.PersonId INTO g RETURN { Customer:c, Recency:MAX(g[*].order.OrderDate), Frequency:LENGTH(g[*]),Monetary:SUM(g[*].order.TotalPrice)}";
		    ArangoDB Conn=(ArangoDB)this.Connection();
			long millisStart10 = System.currentTimeMillis();
			ArangoCursor<String> cursor10 = Conn.db("_system").query(AQ10,String.class);
		    long millisEnd10 = System.currentTimeMillis();
		    System.out.println("Query 10 took "+(millisEnd10 - millisStart10) + " ms");
	    } 
}
