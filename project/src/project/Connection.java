package project;

import com.datastax.driver.core.*;

public class Connection {
	private Cluster cluster;
	private Session session;
	
	public Connection(){
		cluster = Cluster.builder().addContactPoints("127.0.0.1").build();
		session = cluster.connect();
	}
	
	public void connect(String node, String keyspaceName){
		cluster = Cluster.builder().addContactPoints(node).build();
		session = cluster.connect(keyspaceName);
		//session = cluster.connect();
	}
	
	public Session getSession(){
		return this.session;
	}
	
	public void close(){
		session.close();
		cluster.close();
	}
}
