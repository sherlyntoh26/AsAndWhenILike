package project;

import java.util.ArrayList;
import java.util.Date;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class NewOrderTransaction {
	// private attributes
	// private PreparedStatement warehouseQuery;
	private PreparedStatement newOrderQuery;
	private PreparedStatement newOrderLineQuery;
	private PreparedStatement getStockQuery;
	private PreparedStatement getCustomerQuery;
	private Session session;

	public NewOrderTransaction() {
		// public constructor
	}

	// overload constructor
	public NewOrderTransaction(Connection connection) {
		connection.getSession();

		// initiate query
		newOrderQuery = session.prepare(
				"INSERT INTO orderTable(o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d, o_total_amount, o_delivery_d) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		newOrderLineQuery = session.prepare(
				"INSERT INTO orderLine(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_amount, ol_supply_w_id, ol_quantity, ol_dist, ol_i_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		getStockQuery = session.prepare(
				"SELECT i_quantity, i_ytd, i_order_cnt, i_remote_cnt, i_price, i_name FROM inventory WHERE i_id = ? AND i_w_id = ?;");
		getCustomerQuery = session.prepare(
				"SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id =?;");
	}

	public void newOrder(int cWID, int cDID, int cID, int numOfItems, int[] itemNumberArr, int[] sWarehouseID,
			int[] quantities) {

		String dNextOID;
		String dTaxID;
		String district;
		if (cDID == 10) {
			dNextOID = "wdi_d_next_o_id_10";
			dTaxID = "wdi_d_tax_10";
			district = "10";
		} else {
			dNextOID = "wdi_d_next_o_id_0" + cDID;
			dTaxID = "wdi_d_tax_0" + cDID;
			district = Integer.toString(cDID);
		}

		// Select next available order number D_NEXT_O_ID_## from
		// warehouseDistrictInfo based on warehouseID
		String warehouseDistrictSelect = "SELECT wdi_w_tax," + dNextOID + "," + dTaxID
				+ " FROM warehouseDistrictInfo WHERE wdi_w_id = " + cWID + ";";
		ResultSet results = session.execute(warehouseDistrictSelect);
		Row rowWarehouse = results.one();
		int nextOrderID = rowWarehouse.getInt(dNextOID);
		float wTax = rowWarehouse.getFloat("wdi_w_tax");
		float dTax = rowWarehouse.getFloat(dTaxID);

		// Update D_NEXT_O_ID --> increase by 1
		session.execute(String.format("UPDATE warehouseDistrictInfo SET " + dNextOID + " = %d WHERE wdi_w_id = %d ;",
				nextOrderID + 1, cWID));

		// create New order here
		// check whether items are all local
		int allLocal = 1;
		// for (int i = 0; i < sWarehouseID.length; i++) {
		// if (sWarehouseID[i] != cWID) {
		// allLocal = 0;
		// break;
		// }
		// }

		float totalAmt = 0.0f;
		ArrayList<String> arrayListOutput = new ArrayList<String>();
		for (int i = 0; i < numOfItems; i++) {

			if (sWarehouseID[i] != cWID) {
				allLocal = 0;
			}

			results = session.execute(getStockQuery.bind(itemNumberArr[i], sWarehouseID[i]));
			Row rowStock = results.one();
			int stockQty = rowStock.getInt("i_quantity");
			int i_ytd = rowStock.getInt("i_ytd");
			int i_order_cnt = rowStock.getInt("i_order_cnt");
			int i_remote_cnt = rowStock.getInt("i_remote_cnt");
			float i_price = rowStock.getFloat("i_price");
			String i_name = rowStock.getString("i_name");
			float itemAmt = (quantities[i] * i_price);
			totalAmt += itemAmt;

			int adjustedQty = stockQty - quantities[i];
			if (adjustedQty < 10) {
				adjustedQty += 100;
			}

			if (sWarehouseID[i] != cWID) {
				i_remote_cnt += 1;
			}
			session.execute(String.format(
					"UPDATE invetory SET i_quantity = %d, i_ytd =%d, i_order_cnt = %d, i_remote_cnt = %d WHERE i_w_id = %d and i_id = %d;",
					adjustedQty, i_ytd + quantities[i], i_order_cnt + 1, i_remote_cnt, cWID, itemNumberArr[i]));

			// insert into orderLine
			session.execute(newOrderLineQuery.bind(cWID, cDID, nextOrderID, i, itemNumberArr[i], itemAmt,
					sWarehouseID[i], quantities[i], "S_DIST" + district, i_name));

			arrayListOutput.add(String.format("Item: %d | %s, Warehouse %d. Quantity: %d. Aount: %.2f, Stock: %d",
					itemNumberArr[i], i_name, sWarehouseID, quantities[i], itemAmt, adjustedQty));
		}

		// calculate total amount
		// insert into order table
		results = session.execute(getCustomerQuery.bind(cWID, cDID, cID));
		Row rowCustomer = results.one();
		float cDiscount = rowCustomer.getFloat("c_discount");
		String lastName = rowCustomer.getString("c_last");
		String credit = rowCustomer.getString("c_credit");
		totalAmt = totalAmt * (1 + dTax + wTax) * (1 - cDiscount);
		Date orderDate = new Date();
		session.execute(newOrderQuery.bind(cWID, cDID, nextOrderID, cID, null, numOfItems, allLocal, orderDate,
				totalAmt, null));

		// output for this transaction
		System.out.println(String.format("Customer %s, %s, %.2f", lastName, credit, cDiscount));
		System.out.println(String.format("Warehouse Tax: %.2f, District tax: %.2f", wTax, dTax));
		System.out.println(String.format("Order Number: %d, Entry Date: %s", nextOrderID, orderDate));
		System.out.println(String.format("Number of Item: %d, Total amount: %.2f", numOfItems, totalAmt));

		for (String str : arrayListOutput) {
			System.out.println(str);
		}
	}

	// for new order transaction running alone.
	// debuggin use
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// Connection connection = new Connection();
		// connection.connect("127.0.0.1", "project");

		// insert fake data into database
		// NewOrderTransaction newOrder = new NewOrderTransaction(connection);
		// Date since = new Date();
		// String invStmt = "INSERT INTO inventory() VALUES ()";
		// String warehouseDistrictInfoStmt = "INSERT INTO
		// warehouseDistrictInfo() VALUES ()";
		// String customerStmt = "INSERT INTO customer(c_w_id, c_d_id, c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount,c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data, c_w_name, c_d_name) VALUES(";
		// newOrder.session.execute(customerStmt.bind(1, 1, 331, "first",
		// "middle", "last", "street1", "street2", "city", "state", "zip",
		// "phone", since, "GC", 50000, 0.3059, -10, 10.0f, 1, 0, "data",
		// "warehouseName", "districtName"));
		// newOrder.session.execute(customerStmt + "1, 1, 331, first, middle, last, street1, street2, city, state, zip, phone, "+ since + ", GC, 50000, 0.3059, -10, 10, 1, 0 data, warehouseName, districtName)");

		// create transaction objects here
		// newOrder.newOrder(wid, did, cid, noOfItem, itemID, supplyWID,
		// quantity);
	}

}
