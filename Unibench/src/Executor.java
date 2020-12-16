import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import com.arangodb.*;

public class Executor {

	public static String randomChoice(String[] array) {
	    java.util.Random random = new java.util.Random();
	    int index = random.nextInt(array.length);
	    return array[index];
	}
	
	public static void main(String[] args) {
        System.out.println("This is a benchmark for multi-model database, Version 0.1\n" +
                "by UDBMS group of University of Helsinki, Zhang Chao (chao.z.zhang@helsinki.fi), have fun!");
        System.out.println();
       
        System.out.println("The benchmarking is starting...");
        System.out.println("----------------");
        System.out.println("----------------");
        System.out.println("----------------");
        String database = args[0].toString();
        String Query = args[1].toString();
        MMDB db = null;      
        
        switch(database){
        	case "ArangoDB":
        		db = new Arango();
        		break;
        	case "OrientDB":
        		db = new OrientDB();
        	case "AgensGraph":
        		db = new AgensGraph();
        }         
    
        switch(Query) {
        	case "Q1":
                List<String> P1;
        		try {
        			Path currentDir = Paths.get(".");
        			P1 = Files.readAllLines(Paths.get(currentDir.toAbsolutePath()+"/Unibench/PersonIds"));
        			String personId = randomChoice(P1.toArray(new String[0]));
        			db.Q1(personId);
        		} catch (Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "Q2":
                List<String> P2;
        		try {
        			Path currentDir = Paths.get(".");
        			P2 = Files.readAllLines(Paths.get(currentDir.toAbsolutePath()+"/Unibench/ProductIds"));
        			String productId = randomChoice(P2.toArray(new String[0]));
        			db.Q2(productId);
        		} catch (Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "Q3":
                List<String> P3;
        		try {
        			Path currentDir = Paths.get(".");
        			P3 = Files.readAllLines(Paths.get(currentDir.toAbsolutePath()+"/Unibench/ProductIds"));
        			String productId = randomChoice(P3.toArray(new String[0]));
        			db.Q3(productId);
        		} catch (Exception e) {
        			e.printStackTrace();
        		}
			break;
        	case "Q4":
                List<String> P4;
        		try {
        			db.Q4();
        		} catch (Exception e) {
        			e.printStackTrace();
        		}
			break;
        	case "Q5":
                List<String> P5_1;
                List<String> P5_2;
        		try {
        			Path currentDir = Paths.get(".");
        			P5_1 = Files.readAllLines(Paths.get(currentDir.toAbsolutePath()+"/Unibench/PersonIds"));
        			P5_2 = Files.readAllLines(Paths.get(currentDir.toAbsolutePath()+"/Unibench/Brands"));
        			String personId = randomChoice(P5_1.toArray(new String[0]));
        			String brand = randomChoice(P5_2.toArray(new String[0]));
        			db.Q5(personId,brand);
        		} catch (Exception e) {
        			e.printStackTrace();
        		}	
			break;
        	case "Q6":
                List<String> P6;
        		try {
        			Path currentDir = Paths.get(".");
        			P6 = Files.readAllLines(Paths.get(currentDir.toAbsolutePath()+"/Unibench/PersonIds"));
        			String src = randomChoice(P6.toArray(new String[0]));
        			String dst = randomChoice(P6.toArray(new String[0]));
        			db.Q6(src, dst);
        		} catch (Exception e) {
        			e.printStackTrace();
        		}	
			break;
        	case "Q7":
                List<String> P7;
        		try {
        			Path currentDir = Paths.get(".");
        			P7 = Files.readAllLines(Paths.get(currentDir.toAbsolutePath()+"/Unibench/Brands"));
        			String brand = randomChoice(P7.toArray(new String[0]));
        			db.Q7(brand);
        		} catch (Exception e) {
        			e.printStackTrace();
        		}	
			break;
        	case "Q8":
        		try {
        			db.Q8();
        		} catch (Exception e) {
        			e.printStackTrace();
        		}	
			break;
        	case "Q9":
        		try {
        			db.Q9();
        		} catch (Exception e) {
        			e.printStackTrace();
        		}	
			break;
        	case "Q10":
        		try {
        			db.Q10();
        		} catch (Exception e) {
        			e.printStackTrace();
        		}	
			break;       		
      }
	}
}
