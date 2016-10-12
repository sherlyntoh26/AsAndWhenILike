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
		
		String stmt = "SELECT c_first, c_middle, c_last FROM project.customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id =123;";
		ResultSet result = session.execute(stmt);
		System.out.println("first" + result.one().getString("c_first"));
	}
}
// the top one jus ignore. i testing only. 
// start from the string stmt ...
// this one can run & get result u see popularItemTransaction will miss row0 anot ah, tested woud;nt right nono. .one() will take the 1st row HAHAH, so it returned "first" only for result.one?"
// nop. it will return rows of "c_first, c_middle, c_last" --> but with condition it will only return 1 row. so .one() take the 1st row. thn getString("c_first") take the value in this column
//kk u see t