package project;

import com.datastax.driver.core.*;

public class Connection {
	private Cluster cluster;
	private Session session;
	
	public void connect(String node, String keyspaceName){
		cluster = Cluster.builder().addContactPoints(node).build();
		session = cluster.connect(keyspaceName);
	}
	
	public Session getSession(){
		return this.session;
	}
	
	public void close(){
		session.close();
		cluster.close();
	}
}
