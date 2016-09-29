package project;

import com.datastax.driver.core.*;

public class testing {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Cluster cluster = Cluster.builder().addContactPoints("127.0.0.1").build();
		Session session = cluster.connect();
		
//		String cql = "DROP KEYSPACE myfirstcassandradb";
//		session.execute(cql);
//		
//		String cqlStatement = "CREATE KEYSPACE IF NOT EXISTS myfirstcassandradb WITH " + 
//				"replication = {'class':'SimpleStrategy','replication_factor':1}";        
//		session.execute(cqlStatement);
//
//		String cqlStatement2 = "CREATE TABLE IF NOT EXISTS myfirstcassandradb.users (" + 
//				" user_name varchar PRIMARY KEY," + 
//				" password varchar " + 
//				");";
//		session.execute(cqlStatement2);

		System.out.println("Done");
		System.exit(0);
	}

}
