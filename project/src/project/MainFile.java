package project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import com.datastax.driver.core.*;

public class MainFile {
	private boolean usingD8;
	private int noOfNodes;
	private int transactionFileNumber;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		boolean usingD8;
		int noOfNodes;
		int transactionFileNumber;
		if (args == null || args.length <= 0) {
			usingD8 = true;
			noOfNodes = 1;
			transactionFileNumber = 0;
		} else {
			usingD8 = (args[0].trim()).equals("D8");
			noOfNodes = Integer.parseInt(args[1]);
			transactionFileNumber = Integer.parseInt(args[2]);
		}

		for (int i = 0; i < transactionFileNumber; i++) {
			MainFile mf = new MainFile(usingD8, noOfNodes, i);
			mf.runTransactions();
		}
	}

	public MainFile(boolean usingD8, int noOfNodes, int transactionFileNumber) {
		this.usingD8 = usingD8;
		this.noOfNodes = noOfNodes;
		this.transactionFileNumber = transactionFileNumber;
	}

	public void runTransactions() {
		// get connection
		Connection connection = new Connection();
		connection.connect("127.0.0.1", "project");

		// create transaction objects here

		// get transaction file.
		String path = "../data/D%d-xact/%d.txt";
		File file = new File(String.format(path, usingD8 ? 8 : 40, transactionFileNumber));
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String inputLine = reader.readLine();
			while (inputLine != null && inputLine.length() > 0) {
				String[] currentLine = inputLine.split(",");
				if (inputLine.charAt(0) == 'N') {
					// new order transaction --> M+1 lines, M --> number of Items
					// 1st line is 5 comma: N, C_ID, W_ID, D_ID, M
					int cid = Integer.parseInt(currentLine[1]);
					int wid = Integer.parseInt(currentLine[2]);
					int did = Integer.parseInt(currentLine[3]);
					int noOfItem = Integer.parseInt(currentLine[4]);
					
					// create array to store item information
					int[] itemID = new int[noOfItem];
					int[] supplyWID = new int[noOfItem];
					int[] quantity = new int[noOfItem];
					
					// each M line = item 
					// 3 comma: OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY
					String nextLine;
					String[] nLine;
					for(int i=0; i<noOfItem; i++){
						nextLine = reader.readLine();
						nLine = nextLine.split(",");
						itemID[i] = Integer.parseInt(nLine[0]);
						supplyWID[i] = Integer.parseInt(nLine[1]);
						quantity[i] = Integer.parseInt(nLine[2]);
					}
					// send to Order object to do the insertion to DB
					

				} else if (inputLine.charAt(0) == 'P') {
					// payment transaction --> 1 line
					// 5 comma: P, C_W_ID, C_D_ID, C_ID, PAYMENT
					int customerWID = Integer.parseInt(currentLine[1]);
					int customerDID = Integer.parseInt(currentLine[2]);
					int customerID = Integer.parseInt(currentLine[3]);
					float paymentAmt = Float.parseFloat(currentLine[4]);
					
					// send to Payment object to do the update of customer in DB

				} else if (inputLine.charAt(0) == 'D') {
					// delivery transaction --> 1 line
					// 3 comma: D, W_ID, CARRIER_ID
					int warehouseID = Integer.parseInt(currentLine[1]);
					int carrierID = Integer.parseInt(currentLine[2]);
					
					// send to delivery object to do the update of delivery in DB

				} else if (inputLine.charAt(0) == 'O') {
					// order - status transaction --> 1 line
					// 4 comma: O, C_W_ID, C_D_ID, C_ID
					int customerWID = Integer.parseInt(currentLine[1]);
					int customerDID = Integer.parseInt(currentLine[2]);
					int customerID = Integer.parseInt(currentLine[3]);
					
					// send to order-status object to query the status of the last order of a customer
					
				} else if (inputLine.charAt(0) == 'S') {
					// stock - level transaction --> 1 line
					// 5 comma: S, W_ID, D_ID, T, L
					int warehouseID = Integer.parseInt(currentLine[1]);
					int districtID = Integer.parseInt(currentLine[2]);
					int stockThreshold = Integer.parseInt(currentLine[3]);
					int noOfLastOrders = Integer.parseInt(currentLine[4]);
					
					// send to stock-level object to check stock level below specified threshold

				} else if (inputLine.charAt(0) == 'I') {
					// popular - item transaction --> 1 line
					// 4 comma: I, W_ID, D_ID, L
					int warehouseID = Integer.parseInt(currentLine[1]);
					int districtID = Integer.parseInt(currentLine[2]);
					int noOfLastOrders = Integer.parseInt(currentLine[3]);
					
					// send to popular-item object to check 

				} else if (inputLine.charAt(0) == 'T') {
					// top - balance transaction --> 1 line
					// 1 value only
					
					// send to top-balance transaction 
				} else {
					System.out.println("xact wrong format");
				}
				System.out.println("done 1 transaction");
				inputLine = reader.readLine();
			}
			reader.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		//after read file --> close connection
		connection.close();
	}
}
