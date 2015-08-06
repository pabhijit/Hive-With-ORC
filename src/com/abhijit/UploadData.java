package com.abhijit;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class UploadDataThread implements Runnable {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String inputPath = "/home/hdp/Input";
	public volatile static int iCount=0;
	private String tableName;
	private String tempTable;
	private String fileName;

	public UploadDataThread(String currFile, String tableName, String tempTable) {
		this.tempTable = tempTable;
		this.tableName = tableName;
		fileName = currFile;
	}

	public void run() {

		Connection hiveConn = null;
		String sql = null;
		ResultSet res = null;
		try{

			try {
				Class.forName(driverName);
			} catch (ClassNotFoundException e) {
				System.out.println("Hive JDBC driver could not loaded.");
				System.exit(1);
			}
			//Note: Before: hive --service hiveserver2 &
			hiveConn = DriverManager.getConnection("jdbc:hive2://devhdp01.valuecentric.com:10000/default", "", "");

			Statement stmt = hiveConn.createStatement();
			
			//exit if the table already exists.
			sql = "SHOW TABLES LIKE '" + tableName + "'";
			res = stmt.executeQuery(sql);
			if ( !res.next() ) {

				String appendString = null;
				if(tableName.contains("CUSTOMER")) {
					appendString = "(CUSTOMER VARCHAR(10), CUST_NAME1 VARCHAR(40), DEA_NUM VARCHAR(20), HIN_NUM VARCHAR(10), POSTAL_CD1 VARCHAR(10), SALES_DIST1 VARCHAR(6)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'";
				} else if( tableName.contains("INVENTORY") ) {
					appendString = "(VENDOR VARCHAR(10), PLANT VARCHAR(4), MATERIAL VARCHAR(18), WS_DATAB VARCHAR(8), EDI_TRANS_DTE VARCHAR(8), ON_HAND_QTY DECIMAL(9,3)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'";
				} else if( tableName.contains("VENDOR") ) {
					appendString = "(VENDOR VARCHAR(10),VENDOR_NAM VARCHAR(40),BYR_CD VARCHAR(3),BYR_NAM VARCHAR(20)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'";
				} else if( tableName.contains("SALES") ) {
					appendString = "(BILL_DATE VARCHAR(8), BILL_NUM VARCHAR(10),BILL_ITEM VARCHAR(6),CREATEDON VARCHAR(8),PLANT VARCHAR(4), SOLD_TO VARCHAR(10),ZSALES_BLND VARCHAR(1),VENDOR VARCHAR(10)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'";
				} 
				//creating temp table and main table.
				stmt.execute("CREATE TABLE " + tempTable + appendString + " STORED AS TEXTFILE");
				//System.out.println("Running: CREATE TABLE " + tempTable + appendString + " STORED AS TEXTFILE");
				stmt.execute("CREATE TABLE " + tableName + appendString + " STORED AS ORC TBLPROPERTIES (\"ORC.COMPRESS\"=\"ZLIB\")");
				//System.out.println("Running: CREATE TABLE " + tableName + appendString + " STORED AS ORC TBLPROPERTIES (\"ORC.COMPRESS\"=\"ZLIB\")");
				
				// load data into table
				String inputFile = inputPath + "/" + fileName;
				sql = "LOAD DATA LOCAL INPATH '" + inputFile + "' OVERWRITE INTO TABLE " + tempTable;
				System.out.println("Running: " + sql);
				stmt.execute(sql);

				sql = "INSERT OVERWRITE TABLE " + tableName + " SELECT * FROM " + tempTable;
				System.out.println("Running: " + sql);
				stmt.execute(sql);
				
				//drop temp table if it exists.
				stmt.execute("DROP TABLE IF EXISTS " + tempTable);
			}
			hiveConn.close();
			iCount=iCount+1;
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
public class UploadData {

	private static String inputPath = "/home/hdp/Input";
	//private static String inputPath = "C:\\Users\\Abhijit.Pattanaik\\Documents\\ABC Files\\Input Files";
	/**
	 * @param args
	 * @throws SQLException
	 */

	public static void main(String[] args) throws SQLException {

		ExecutorService executor = Executors.newFixedThreadPool(15);
		String currFile = null;
		String tempTable = null;
		String tableName = null;

		int numFiles = 0;
		String tablePattern = "(?i)([\\w]+)(\\..*)";
		//String tempPattern = "(?i)([\\w]+)(\\_.*)";

		try{
			long start = System.currentTimeMillis();

			File folder = new File(inputPath);
			File[] listOfFiles = folder.listFiles();

			for ( ; numFiles < listOfFiles.length; numFiles++) {
				if (listOfFiles[numFiles].isFile()) {
					currFile = listOfFiles[numFiles].getName();

					//tempTable = currFile.replaceAll(tempPattern, "$1")+"_TEXT";
					tableName = currFile.replaceAll(tablePattern, "$1");
					tempTable = tableName + "_TEXT";
					
					//System.out.println( "Launched thread for uploading " + currFile );
					Runnable processCustomer = new UploadDataThread(currFile,tableName,tempTable);
					executor.execute(processCustomer);
				} 
			}

			//Wait for all the threads to finish executing
			while(numFiles!=UploadDataThread.iCount) {

			}

			List<Runnable> lstNotExecuted = executor.shutdownNow();

			if( lstNotExecuted.size() != 0 ) {
				System.out.println( lstNotExecuted.size() +" files could not be processed.");
			} else {
				System.out.println("All files were successfully processed.");
			}

			long end = System.currentTimeMillis();
			System.out.println("Time taken to write " + numFiles + " files: " + (end - start) / 1000f + " seconds");

		} catch( Exception exception ) {
			System.out.println( "Exception in main block : " + exception.getMessage() );
		}
	}
}
