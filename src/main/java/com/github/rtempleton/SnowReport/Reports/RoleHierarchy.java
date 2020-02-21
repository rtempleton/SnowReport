package com.github.rtempleton.SnowReport.Reports;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.rtempleton.SnowReport.SnowReport;
import com.google.gson.Gson;



/***
 * 
 * @author rtempleton
 * Creates a hierarchal output of all ROLES defined in the account with pointers to parent/child ROLES
 * and lists all USERS with that roles priveleges
 * 
 */
public class RoleHierarchy {
	
	private static final Log logger = LogFactory.getLog(RoleHierarchy.class);
	private final Map<String, RoleObj> root = new HashMap<String, RoleObj>();

	
	public RoleHierarchy(Properties props) {
		logger.info(String.format("Initiating Report %s", this.getClass().getSimpleName()));
		
		
		Connection conn = null;
		Statement stmt = null;
		ResultSet rset = null;
		
		try {
			conn = SnowReport.getConnection(SnowReport.getRequiredProperty(props, "URI"),
					SnowReport.getRequiredProperty(props, "USER"),
					SnowReport.getRequiredProperty(props, "PASSWORD"));
			stmt = conn.createStatement();
			
			rset = stmt.executeQuery("show roles");
			
			while(rset.next()) {
				String name = rset.getString(2);
				int number = rset.getInt(7);
				logger.debug(String.format("SHOW ROLES: Role Name: %1$s granted to roles: %2$d", name, number));
				processRole(name, conn);
			}
			rset.close();
		
			
		}catch (Exception e) {
			logger.error(e.getMessage());
		}finally {
			try { if (rset != null) rset.close(); } catch(Exception e) { }
			try { if (stmt != null) stmt.close(); } catch(Exception e) { }
			try { if (conn != null) conn.close(); } catch(Exception e) { }
		}
		
		
		cleanUpRoot(root.get("ACCOUNTADMIN"));
		Gson gson = new Gson();
		logger.info(gson.toJson(root));
		
	}
	
	private void processRole(String roleName, Connection conn) {
		
		if(root.containsKey(roleName))
			return;
		
		RoleObj thisRole = new RoleObj();
		root.put(roleName, thisRole);
		
		Statement stmt = null;
		ResultSet rset = null;
		try {
			stmt = conn.createStatement();
			String query = "show grants of role " + roleName;
			logger.debug("processRole recursion query: " + query);
			rset = stmt.executeQuery(query);
			while(rset.next()) {
				String type = rset.getString(3);
				String name = rset.getString(4);
				if (type.equalsIgnoreCase("USER")) {
					thisRole.addUser(name);
				} else if (type.equalsIgnoreCase("ROLE")) {
					if(!root.containsKey(name)) { //if the parent role isn't in the root
						processRole(name, conn);  //add it to the root
					}
					root.get(name).addChild(roleName, thisRole); //add this to the parent role
					
				}
			}
			rset.close();
			
			//add the database and schema details to the Role object
			query = "show grants to role " + roleName;
			logger.debug("processRole database details: " + query);
			rset = stmt.executeQuery(query);
			while(rset.next()) {
				if (rset.getString(3).equalsIgnoreCase("DATABASE")) {
					String dbName = rset.getString(4); 
					DBObj thisDb = thisRole.getDB(dbName); // get the DBObj from the list in the current Role
					thisDb.addPrivilege(rset.getString(2)); // add the privilege to the list
				} else if (rset.getString(3).equalsIgnoreCase("SCHEMA")) {
					String foo = rset.getString(4);
					String[] tmp = foo.split("\\.");
					String dbName = tmp[0];
					String schemaName = tmp[1];
					SchemaObj thisSchema = thisRole.getDB(dbName).getSchema(schemaName); // get the SchemaObj from the DBObj from the list in the current Role
					thisSchema.addPrivilege(rset.getString(2)); // add the privilege
				}
				
			}
			rset.close();
			
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} finally {
			try { if (rset != null) rset.close(); } catch(Exception e) { }
			try { if (stmt != null) stmt.close(); } catch(Exception e) { }
		}
		
	}
	
	//removes nested roles from the root
	private void cleanUpRoot(RoleObj role) {
		for (String name : role.getChildren().keySet()) { //iterate through the child roles of this role
			root.remove(name); //remove the reference from the root
			cleanUpRoot(role.getChildren().get(name)); //recurse to the child role 
		}
		
	}
	
	
	private class RoleObj {
		
		public final Set<String> users = new TreeSet<String>();
		public final Map<String, RoleObj> childRoles = new HashMap<String, RoleObj>();
		public final Map<String, DBObj> databases = new HashMap<String, DBObj>();
		
		
		public void addChild(String name, RoleObj child) {
			this.childRoles.put(name, child);
		}
		
		public Map<String, RoleObj> getChildren(){
			return this.childRoles;
		}
		
		public void addUser(String user) {
			this.users.add(user);
		}
		
		public Set<String> getUsers(){
			return users;
		}
		
		public void addDB(String name, DBObj db) {
			this.databases.put(name, db);
		}
		
		public DBObj getDB(String name){
			if(databases.containsKey(name)) {
				return databases.get(name);
			}
			DBObj db = new DBObj();
			this.addDB(name, db);
			return db;
		}

		
	}
	
	private class DBObj {
		
		private final Map<String, SchemaObj> schemas = new HashMap<String, SchemaObj>();
		private final Set<String> dbPrivileges = new TreeSet<String>();
		
		
		public void addSchema(String name, SchemaObj schema) {
			this.schemas.put(name, schema);
		}
		
		public SchemaObj getSchema(String name){
			if(schemas.containsKey(name)) {
				return schemas.get(name);
			}
			SchemaObj schema = new SchemaObj();
			this.addSchema(name, schema);
			return schema;
		}
		
		public void addPrivilege(String permission) {
			if (!this.dbPrivileges.contains(permission))
				this.dbPrivileges.add(permission);
		}
		
		
	}
	
	private class SchemaObj {

		private final Set<String> schemaPrivileges = new TreeSet<String>();
		
		
		public void addPrivilege(String permission) {
			if (!this.schemaPrivileges.contains(permission))
				this.schemaPrivileges.add(permission);
		}
		
		
	}

}
