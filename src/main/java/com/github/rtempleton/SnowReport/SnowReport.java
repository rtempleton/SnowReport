package com.github.rtempleton.SnowReport;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.rtempleton.SnowReport.Reports.DatabaseSummary;
import com.github.rtempleton.SnowReport.Reports.RoleHierarchy;



public class SnowReport {

	private static final Log logger = LogFactory.getLog(SnowReport.class);
	private static final String sfc_driver = "net.snowflake.client.jdbc.SnowflakeDriver";
	public Properties props;
	private boolean exit = false;

	public static void main(String[] args) throws Exception {
		new SnowReport(args);
	}

	public SnowReport(String[] args) {
		this.props = SnowReport.readProperties(args[0]);
		try {
			Class.forName(sfc_driver);
		} catch (ClassNotFoundException e) {
			logger.error(e.getMessage());
		}

		System.out.println("Welcome to SnowReport!\n");
		System.out.println("The output from the reports here can be visualized in any JSON Editor");
		System.out.println("A good Chrome plug-in can be found here: https://chrome.google.com/webstore/detail/json-editor/lhkmoheomjbkfloacpgllgjcamhihfaj");
		printHelp();
		
		Scanner in = new Scanner(System.in);
		String s;
		while(!exit) {
			System.out.println("\nWhat is your command?");
			s = in.nextLine();

			switch(s.toUpperCase()) {
			case "HELP":
				printHelp();
				break;
			case "ROLEHIERARCHY":
				new RoleHierarchy(props);
				break;
			case "DATABASESUMMARY":
				new DatabaseSummary(props);
				break;
			case "QUIT":
				exit = true;
				System.out.println("Quitting SnowReport");
				break;
			default:
				System.out.println(String.format("The command \"%s\" was not recognized. Try again", s));
				printHelp();
				break;
			}
		}
		in.close();
	}

	private void printHelp() {
		System.out.print("\nThe following commands are available:\n"
				+ "help - Shows this message\n"
				+ "RoleHierarchy - Creates a tree of ROLES defined in account, lists the USERS and also details all Database and Schema privileges for the given role\n"
				+ "DatabaseSummary - Creates a tree of all DATABASES in the account and details all sequences, streams, procedures, functions, tasks, stages, and pipes within each SCHEMA\n"
				+ "quit - Quits SnowReport\n");
	}


	public static Properties readProperties(String propsPath){
		Properties props = new Properties();
		try {
			props.load(new FileInputStream(propsPath));
		} catch (Exception e) {
			logger.error(e.getStackTrace());
			System.exit(1);
		}
		return props;
	}


	public static String getRequiredProperty(Properties props, String name){
		if(!props.containsKey(name)){
			logger.error(String.format("The required property %s was not found in your configuration file. Please check the value and try again.", name));
			System.exit(-1);
		}
		return props.getProperty(name).trim();
	}


	public static Connection getConnection(String URI, String UID, String PWD) throws Exception {
		return DriverManager.getConnection(URI, UID, PWD);
	}


//	public static Properties parseArgs(String[] args) {
//		Properties p = new Properties();
//		for (String s : args) {
//			int pos = s.indexOf('=');
//			p.put(s.substring(0, pos), s.substring(pos+1, s.length()));
//		}
//		return p;
//	}




}
