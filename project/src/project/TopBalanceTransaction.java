package project;

import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class TopBalanceTransaction {
	private Session session;
	private PreparedStatement selectCustomerStmt;

	public TopBalanceTransaction(){
		// public constructor
	}
	
	public TopBalanceTransaction(Connection connection){
		session = connection.getSession();
		selectCustomerStmt = session.prepare("SELECT c_first, c_middle, c_last, c_balance, c_w_name, c_d_name FROM customer ORDER BY c_balance DESC LIMIT 10;");
	}
	
	public void getTopbalance(){
		ResultSet customerResult = session.execute(selectCustomerStmt.bind());
		List<Row> customerRow = customerResult.all();
		String printStmt = "%d. Name: (%s, %s, %s) | Balance: %.2f | Warehouse name: %s | District name: %s";
		int noOfCust = 1;
		
		for(int i = 0; i < customerRow.size(); i++){
			Row currentRow = customerRow.get(i);
			System.out.println(String.format(printStmt, noOfCust, currentRow.getString("c_first"), currentRow.getString("c_middle"), currentRow.getString("c_last"), currentRow.getDecimal("c_balance"), currentRow.getString("c_w_name"), currentRow.getString("c_d_name")));
			noOfCust++;
		}
	}
}
