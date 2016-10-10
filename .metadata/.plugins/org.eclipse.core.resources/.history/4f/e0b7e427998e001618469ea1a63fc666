package project;

import java.util.Date;

import com.datastax.driver.core.*;

public class testing {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Date date = new Date();
		System.out.println("date1:" + date);
		
		Cluster cluster = Cluster.builder().addContactPoints("127.0.0.1").build();
		Session session = cluster.connect();
		
//		String cql = "DROP KEYSPACE myfirstcassandradb";
//		session.execute(cql);
//		
		String cqlStatement = "CREATE KEYSPACE IF NOT EXISTS myfirstcassandradb WITH " + 
				"replication = {'class':'SimpleStrategy','replication_factor':1}";        
		session.execute(cqlStatement);

		String cqlStatement2 = "CREATE TABLE IF NOT EXISTS myfirstcassandradb.users (" + 
				" user_name varchar PRIMARY KEY," + 
				" password varchar " + 
				");";
		session.execute(cqlStatement2);
		
		String cqlStatement3 = "INSERT INTO myfirstcassandradb.users(user_name, password) VALUES('lyn', 'lyn')";
		session.execute(cqlStatement3);

		System.out.println("Done");
		//System.exit(0);
		
		
		
		Date endDate = new Date(); 
		System.out.println("date2: " + endDate);
		
		System.out.println(String.format("%d", (endDate.getTime() - date.getTime())/1000));
		
		int dID = 10;
		String dIDStr = String.format("%02d", dID);
		System.out.println(dIDStr);
	}
}
