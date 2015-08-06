package com.abhijit;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class ProcessFiles implements Runnable {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public volatile static int iCount=0;
	private String timestamp;
	private String customer;
	public ProcessFiles(String time, String custId) {
		timestamp = time;
		customer = custId;
	}

	public void run() {
		processSalesfiles( timestamp,customer );
		iCount=iCount+1;
	}
	private static void processSalesfiles( String timestamp, String customerId ) {

		PrintWriter writer = null;
		String query = null;
		PreparedStatement statement = null;
		Connection hiveConn;

		try{

			try {
				Class.forName( driverName );
			} catch ( ClassNotFoundException e ) {
				System.out.println( "Hive JDBC driver could not loaded." );
				System.exit(1);
			}
			hiveConn = DriverManager.getConnection( "jdbc:hive2://devhdp01.abhijit.com:10000/default", "", "" );

			query = "SELECT VC.ID,PLS.PLANT, PLS.SOLD_TO,C.CUSTCOT_NAME1,C.STREET1, C.CITY1,C.REGION1 "
					+ "FROM SALES_" + timestamp + " AS PLS "
					+ "INNER JOIN CUSTOMER_" + timestamp + " AS C ON (C.CUSTOMER=PLS.SOLD_TO) "
					+ "INNER JOIN CUSTOMER_VENDOR_MAP AS AVR ON (AVR.VENDOR_ID = PLS.VENDOR) "
					+ "INNER JOIN VTRAK_CUSTOMER AS VC ON (VC.ID = AVR.CUST_ID) WHERE AVR.CUST_ID = ?";
			statement = hiveConn.prepareStatement(query);
			statement.setString(1, customerId);
			ResultSet rset = statement.executeQuery();

			//Write the data on to a file
			writer = new PrintWriter("/home/hdp/output/man_"+ timestamp + "_" + customerId + ".txt", "UTF-8");
			
			while (rset.next()) {
				writer.println(rset.getString(1) + "|" + rset.getString(2) + "|"+"91"+"|" + rset.getString(3) + "|" + rset.getString(4) +"|"+"91"+"|" + rset.getString(5) + "||" + rset.getString(6) + "|" + rset.getString(7) + "|" + rset.getString(8) + "|||||" + rset.getString(9) + "|" +"IN" + "|"+ rset.getString(10) + "|||||" + rset.getString(11) + "|||" + rset.getString(12) + rset.getString(13) + rset.getString(13) +"||" + rset.getString(14) +"|||" );
			}

			rset.close();
			hiveConn.close();
		} catch( Exception ex ) {
			ex.printStackTrace();
		} finally {

			writer.flush();
			writer.close();
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
	}
}

public class WriteToFile {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String args[]) throws Exception {

		Connection hiveConn;
		int numRecords = 0;
		ResultSet rset = null;
		ExecutorService executor = Executors.newFixedThreadPool(15);
		List<String> lstTimestamps = new ArrayList<>();
		List<String> lstCustomers = new ArrayList<>();
		try {

			long start = System.currentTimeMillis();
			try {
				Class.forName(driverName);
			} catch (ClassNotFoundException e) {
				System.out.println("Hive JDBC driver could not loaded.");
				System.exit(1);
			}

			hiveConn = DriverManager.getConnection("jdbc:hive2://devhdp01.valuecentric.com:10000/default", "", "");
			System.out.println("Connection established.."); //connected

			Statement stmt = hiveConn.createStatement();
			
			rset = stmt.executeQuery("SHOW TABLES LIKE 'CUSTOMER_*'");
			while ( rset.next() ) {
				if(!rset.getString(1).equalsIgnoreCase("CUSTOMER_VENDOR_MAP")) {
					String[] temp = rset.getString(1).split("_");
					lstTimestamps.add( temp[1] );
				}
			}
			
			//Fetch customer ids
			rset = hiveConn.createStatement().executeQuery("SELECT ID FROM CUSTOMER");

			while( rset.next() ) {
				lstCustomers.add( rset.getString( 1 ) );
			}
			Iterator<String> iterCustomer = lstCustomers.iterator();
			for( String time: lstTimestamps ) {
			while( iterCustomer.hasNext() ) {
				Runnable processCustomer = new ProcessFiles( time, iterCustomer.next() );
				executor.execute(processCustomer);
				numRecords++;
			}
			iterCustomer = lstCustomers.iterator();
			}
			//Wait for all the threads to finish executing
			while( numRecords!=ProcessFiles.iCount ) {

			}
			rset.close();
			hiveConn.close();
			List<Runnable> lstNotExecuted = executor.shutdownNow();

			//Verify the number of files processed against the number of customers
			if( lstNotExecuted.size() != 0 ) {
				System.out.println( lstNotExecuted.size() +" files could not be processed.");
			} else {
				System.out.println("All files were successfully processed.");
			}

			long end = System.currentTimeMillis();
			System.out.println("Time taken to write " + numRecords + " files: " + (end - start) / 1000f + " seconds");
		} catch( Exception ex ) {
			ex.printStackTrace();
		}
	}
}
