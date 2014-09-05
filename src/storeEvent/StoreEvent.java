package storeEvent;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class StoreEvent {

	private static Logger log = Logger.getLogger(StoreEvent.class);
	private static Connection conn;
	private final static String app_ver = "StoreEvent v 1.30";

	public static void main(String[] args) {
		
		loadProperties();
		
		log.setLevel(Level.INFO);	
		log.log(Level.INFO, "Starting application "+ app_ver);

        String username = props.getProperty("jdbc.username");
        String password = props.getProperty("jdbc.password");
        String db = props.getProperty("jdbc.database");
        String server = props.getProperty("jdbc.server");
        int cycle = Integer.parseInt(props.getProperty("server.cycle"));
        int limit = Integer.parseInt(props.getProperty("server.limit"));
        int start = Integer.parseInt(props.getProperty("server.start"));
        int repl_cycle = Integer.parseInt(props.getProperty("replication.cycle"));
        int repl_limit = Integer.parseInt(props.getProperty("replication.limit"));
        String hbase_ip = props.getProperty("hbase.ip");
        String hbase_table_event = props.getProperty("hbase.table.event");

        String loglevel = props.getProperty("log.level");

		String url = "jdbc:postgresql://" + server + "/" + db;
		Properties c_props = new Properties();
		c_props.setProperty("user",username);
		c_props.setProperty("password",password);
		c_props.setProperty("SSL","FALSE");
		
		if (loglevel.equalsIgnoreCase("DEBUG")) {
			log.setLevel(Level.DEBUG);
			log.log(Level.INFO, "Setting loglevel to DEBUG");
		}
		if (loglevel.equalsIgnoreCase("TRACE")) {
			log.setLevel(Level.TRACE);
			log.log(Level.INFO, "Setting loglevel to TRACE" + "");
		}

		try {

	        Class.forName("org.postgresql.Driver");
	         
			conn = DriverManager.getConnection(url, c_props);
			if (conn== null) {
				log.error("No connection to database.");
				System.exit(1);
			}
			
			final StoreProcessor sp = new StoreProcessor(conn, cycle, limit);
			final ReplicationProcessor rp = new ReplicationProcessor(conn, repl_cycle, repl_limit);

			sp.setID(start);
			
			sp.hbase_ip = hbase_ip;
			sp.hbase_table = hbase_table_event;
			
			rp.hbase_ip = hbase_ip;
			rp.hbase_table = hbase_table_event;
			
			Runtime.getRuntime().addShutdownHook(new Thread() {

				public void run() {
			    	try {
						log.log(Level.INFO, "Got signal to stop...");
						// geef bij de processor aan dat'ie moet stoppen
						sp.quit();
						rp.quit();
						log.log(Level.INFO, "stopped processing.");
						sp.join();
						rp.join();
						
						props.setProperty("server.start", Integer.toString(sp.id));
						saveProperties();

						log.log(Level.INFO, "Went down gracefully.");
					} catch (InterruptedException e) {
						log.error("Could not save all." + e.getMessage());
					}
			    }
			 }
			);
			
			if (limit > 0)
				sp.start();
			if (repl_limit > 0)
				rp.start();
			sp.join();
			rp.join();
			
			props.setProperty("server.start", Integer.toString(sp.id));
			saveProperties();
			
			log.log(Level.INFO, "Stopped at : " + sp.id);
			log.log(Level.INFO, "Stopping application.");

		}
		catch (SQLException e) {
			e.printStackTrace();	
		} 
		catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private static Properties props;
	private static void loadProperties()
	{
        props = new Properties();
        try {

			//System.out.println("Working Directory = " +
		    String appdir = System.getProperty("user.dir");
        	log.log(Level.INFO, "Reading properties from dir: " + appdir);
			FileInputStream fis = new FileInputStream(appdir + "/StoreEvent.properties");

			//loading properties from properties file
			props.load(fis);
		} catch (FileNotFoundException e1) {
			log.error("Could not find properties file");
			System.exit(1);
		} catch (InvalidPropertiesFormatException e1) {
			log.error("Wrong Format in properties file");
			System.exit(1);
		} catch (IOException e1) {
			log.error("Error reading properties file");
			e1.printStackTrace();
			System.exit(1);
		}

	}
	
	private static void saveProperties() {
	
		try {
		    String appdir = System.getProperty("user.dir");
        	log.log(Level.INFO, "Writing properties to dir: " + appdir);
			FileWriter fos = new FileWriter(appdir + "/StoreEvent.properties", false);
			props.store(fos, app_ver);
			
		}
		catch (IOException e1) {
			
		}
	}


}
