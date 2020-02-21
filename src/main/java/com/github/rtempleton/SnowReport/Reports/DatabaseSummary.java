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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.rtempleton.SnowReport.SnowReport;
import com.google.gson.Gson;

public class DatabaseSummary {

	private static final Log logger = LogFactory.getLog(DatabaseSummary.class);
	private final Map<String, DBObj> root = new HashMap<String, DBObj>();

	public DatabaseSummary(Properties props) {

		logger.info(String.format("Initiating Report %s", this.getClass().getSimpleName()));
		
		int concurrent = Math.max(Integer.parseInt(SnowReport.getRequiredProperty(props, "CONCURRENT_TASKS")), 1);
		logger.debug(String.format("Running with %d concurrent threads", concurrent));
		int timeout =  Math.max(Integer.parseInt(SnowReport.getRequiredProperty(props, "TIMEOUT_MINUTES")), 1);
		logger.debug(String.format("Timeout set to %d minutes", timeout));

		Connection conn = null;
		Statement stmt = null;
		ResultSet rset = null;

		try {
			conn = SnowReport.getConnection(SnowReport.getRequiredProperty(props, "URI"),
					SnowReport.getRequiredProperty(props, "USER"), SnowReport.getRequiredProperty(props, "PASSWORD"));
			stmt = conn.createStatement();

			rset = stmt.executeQuery("show databases");
			ExecutorService executor = Executors.newFixedThreadPool(concurrent);

			while (rset.next()) {
				executor.submit(new ProcessDB(rset.getString(2), conn));
			}
			rset.close();
			executor.shutdown();
			executor.awaitTermination(timeout, TimeUnit.MINUTES);


		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally {
			try {
				if (rset != null)
					rset.close();
			} catch (Exception e) {
			}
			try {
				if (stmt != null)
					stmt.close();
			} catch (Exception e) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (Exception e) {
			}
		}

		Gson gson = new Gson();
		logger.info(gson.toJson(root));

	}


	private class ProcessDB implements Runnable{
		
		private final String dbName;
		private final Connection conn;
		
		public ProcessDB(String dbName, Connection conn) {
			this.dbName = dbName;
			this.conn = conn;
		}
		
		public void run() {
			
			DBObj thisDb = new DBObj();
			root.put(dbName, thisDb);

			String query = (String.format("select object_catalog, object_schema, object_name, object_type from \"%1$s\".information_schema.object_privileges where object_type in ('SCHEMA', 'SEQUENCE', 'STREAM', 'PROCEDURE', 'FUNCTION', 'TASK')", dbName));

			logger.debug(query);

			Statement stmt = null;
			ResultSet rset = null;

			try {
				stmt = conn.createStatement();
				rset = stmt.executeQuery(query.toString());
				while (rset.next()) {

					DBObj thisDB;
					if(root.containsKey(rset.getString(1))) {
						thisDB = root.get(rset.getString(1));
					}else {
						thisDB = new DBObj();
						root.put(rset.getString(1), thisDB);

					}
					String objName = rset.getString(3);
					String objType = rset.getString(4);

					if (objType.equals("SCHEMA")) { // the schema name is derived from the objName column when the type is
						// schema - handle this case uniquely
						thisDB.getSchema(rset.getString(3));
						continue;
					}

					SchemaObj schema = thisDB.getSchema(rset.getString(2));
					switch (objType) {
					case "SEQUENCE":
						schema.addSequence(objName);
						break;
					case "STREAM":
						schema.addStream(objName);
						break;
					case "PROCEDURE":
						schema.addProcedure(objName);
						break;
					case "FUNCTION":
						schema.addFunction(objName);
						break;
					case "TASK":
						schema.addTask(objName);
						break;
					default:
						logger.warn(String.format("An invalid OBJECT_TYPE value of %1$s was found. This is not a defined type."));
						break;
					}

				}
				rset.close();


				query = (String.format("select 'STAGE', stage_catalog as catalog, stage_schema as schema, stage_name as name, stage_url as url, stage_region as region, stage_type as type, null as definition, null as is_autoingest_enabled, null as notification_channel from \"%1$s\".information_schema.stages\n" + 
						"union\n" + 
						"select 'PIPE', pipe_catalog as catalog, pipe_schema as schema, pipe_name as name, null as url, null as region, null as type, definition, is_autoingest_enabled, notification_channel_name from \"%1$s\".information_schema.pipes\n", dbName));


				logger.debug("processStage & Pipe details: " + query.toString());
				rset = stmt.executeQuery(query.toString());
				while (rset.next()) {
					SchemaObj schema = root.get(rset.getString(2)).getSchema(rset.getString(3));
					String objType = rset.getString(1);
					String name = rset.getString(4);

					switch(objType) {
					case "STAGE":
						StageObj stage = schema.getStage(name);
						stage.setUrl(rset.getString(5));
						stage.setType(rset.getString(7));
						stage.setRegion(rset.getString(6));
						break;
					case "PIPE":
						PipeObj pipe = schema.getPipe(name);
						pipe.setDefinition(rset.getString(8));
						pipe.setAutoingest_enabled(rset.getString(9));
						pipe.setNotification_channel(rset.getString(10));
						break;
					}


				}
				rset.close();

			} catch (SQLException e) {
				logger.error(e.getMessage());
			} finally {
				try {
					if (rset != null)
						rset.close();
				} catch (Exception e) {
				}
				try {
					if (stmt != null)
						stmt.close();
				} catch (Exception e) {
				}
			}
			
		}
	}

	private class DBObj {

		private final Map<String, SchemaObj> schemas = new HashMap<String, SchemaObj>();

		public void addSchema(String name, SchemaObj schema) {
			this.schemas.put(name, schema);
		}

		public SchemaObj getSchema(String name) {
			if (schemas.containsKey(name)) {
				return schemas.get(name);
			}
			SchemaObj schema = new SchemaObj();
			this.addSchema(name, schema);
			return schema;
		}

	}

	private class SchemaObj {

		private final Set<String> sequences = new TreeSet<String>();
		private final Set<String> streams = new TreeSet<String>();
		private final Set<String> procedures = new TreeSet<String>();
		private final Set<String> functions = new TreeSet<String>();
		private final Set<String> tasks = new TreeSet<String>();
		private final Map<String, StageObj> stages = new HashMap<String, StageObj>();
		private final Map<String, PipeObj> pipes = new HashMap<String, PipeObj>();

		public void addSequence(String sequence) {
			this.sequences.add(sequence);
		}

		public void addStream(String stream) {
			this.streams.add(stream);
		}

		public void addProcedure(String procedure) {
			this.procedures.add(procedure);
		}

		public void addFunction(String function) {
			this.functions.add(function);
		}

		public void addTask(String task) {
			this.tasks.add(task);
		}

		public StageObj getStage(String stage) {
			if (stages.containsKey(stage)) {
				return stages.get(stage);
			}
			StageObj stageObj = new StageObj();
			stages.put(stage, stageObj);
			return stageObj;
		}

		public PipeObj getPipe(String pipe) {
			if (pipes.containsKey(pipe)) {
				return pipes.get(pipe);
			}
			PipeObj pipeObj = new PipeObj();
			pipes.put(pipe, pipeObj);
			return pipeObj;
		}

	}

	private class StageObj {

		private String url;
		private String type;
		private String region;

		public void setUrl(String url) {
			this.url = url;
		}

		public void setType(String type) {
			this.type = type;
		}

		public void setRegion(String region) {
			this.region = region;
		}

		public String getUrl() {
			return url;
		}

		public String getType() {
			return type;
		}

		public String getRegion() {
			return region;
		}

	}

	private class PipeObj {

		private String definition;
		private String autoingest_enabled;
		private String notification_channel;

		public String getDefinition() {
			return definition;
		}
		public void setDefinition(String definition) {
			this.definition = definition;
		}
		public String isAutoingest_enabled() {
			return autoingest_enabled;
		}
		public void setAutoingest_enabled(String autoingest_enabled) {
			this.autoingest_enabled = autoingest_enabled;
		}
		public String getNotification_channel() {
			return notification_channel;
		}
		public void setNotification_channel(String notification_channel) {
			this.notification_channel = notification_channel;
		}
	}

}
