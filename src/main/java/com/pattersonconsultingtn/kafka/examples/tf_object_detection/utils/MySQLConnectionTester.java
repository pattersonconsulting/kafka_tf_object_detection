package com.pattersonconsultingtn.kafka.examples.tf_object_detection.utils;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

public class MySQLConnectionTester {


  public static void main(String[] args) {

String connectionString = "jdbc:mysql://localhost:3306/big_cloud_dealz?user=root&password=1234";

	Connection conn = null;

	Statement stmt = null;
	ResultSet rs = null;
	
	try {

		System.out.println("Testng: " + connectionString);
	    conn =
	       DriverManager.getConnection( connectionString );

	           stmt = conn.createStatement();
			    rs = stmt.executeQuery("SELECT count(*) FROM inventory;");

			    rs.close();
			    stmt.close();

		System.out.println("Query completed!");


	    // Do something with the Connection

	   
	} catch (SQLException ex) {
	    // handle any errors
	    System.out.println("SQLException: " + ex.getMessage());
	    System.out.println("SQLState: " + ex.getSQLState());
	    System.out.println("VendorError: " + ex.getErrorCode());
	}


  }


}