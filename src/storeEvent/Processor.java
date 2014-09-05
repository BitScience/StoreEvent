package storeEvent;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public abstract class Processor extends Thread{
	private boolean stop;
	private int max_runtime;
	protected Connection conn;
	protected int count;
	protected int localcount;
	protected int limit;
	protected Statement st;
	
	protected static Logger log = Logger.getLogger(StoreEvent.class);

	abstract void init();
	abstract void proces();
	
	abstract String getDesc();
	
	public Processor(Connection c, int runtime, int l) {

		conn = c;
		max_runtime = runtime;
		count = 0;
		limit = l;
		try {
			st = c.createStatement();
		} catch (SQLException e) {
			log.log(Level.DEBUG, "Error creating Statement : " + e.getMessage());
			quit();
		}

		init();
	}
		
	public void quit(){
		stop = true;
	}
	
	public void run() {
		long proc_start;
		long proc_stop;
		
    	while (!stop) {
 			proc_start = Now();
 	   		proces();
			proc_stop = Now()-proc_start;
			count += localcount;
			
			//if (count > 1000000) stop = true;
			
			if (stop) break;
			
			if (proc_stop < max_runtime) {
				try {
					Thread.sleep(max_runtime-proc_stop);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			log.log(Level.DEBUG,"Next step in " + getDesc() + ". (" + count + ")");
    	}		
	}

	private static long Now() {
		Date d = new Date();
		return d.getTime();
	}

}
