package storeEvent;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.log4j.Level;

import bigdata.HBaseTrackingEvent;
import bigdata.HBaseTrackingEventList;

public class ReplicationProcessor extends Processor {
	
	public String hbase_ip;
	public String hbase_table;
	
	private HashMap<String, Integer> hmUnits;
	private ArrayList<Integer> evList;
	
	public ReplicationProcessor(Connection c, int runtime, int l) {
		super(c, runtime, l);
	}

	private void loadUnits() {
		
		try {
		
			Statement st1 = conn.createStatement();


			ResultSet idunit = st1.executeQuery("SELECT * FROM nazzaunit");
			while (idunit.next()) {
				hmUnits.put(idunit.getString(1), idunit.getInt(2));
			}
			
			idunit.close();
			st1.close();
		}
		catch (SQLException e) {
			log.log(Level.DEBUG, "Error reading units to store events : " + e.getMessage());
			quit();
		}	
	}
	
	private Integer getUnitId(String sc) {
		return hmUnits.get(sc);
	}
	
	@Override
	void init() {
		hmUnits = new HashMap<String, Integer>();
		evList = new ArrayList<Integer>();
		loadUnits();

	}
	
	@Override	
	String getDesc() {
		return "ReplicationProcessor";
	}
	
	@Override
	void proces() {
		try {

				localcount = 0;
				ResultSet rs = st.executeQuery("SELECT * FROM r_mprofevents order by \"ID\" limit " + limit);

				HBaseTrackingEventList hbaselist = new HBaseTrackingEventList(hbase_ip, hbase_table);
						
				while (rs.next()) {

					Event ev = new Event(rs);
					
					//if (ev.Longitude > 48.0 && ev.Longitude < 52.5 && ev.Latitude < ev.Longitude) {  // is iets aan de hand. Lat en Long zijn omgedraaid
					//	Double lat_tmp = ev.Latitude;
					//	ev.Latitude = ev.Longitude;
					//	ev.Longitude = lat_tmp;
					//}
					
					// Store Event In HBase
					Integer[] Data = new Integer[7];
					Data[0] = ev.Data1;
					Data[1] = ev.Data2;
					Data[2] = ev.Data3;
					Data[3] = ev.Data4;
					Data[4] = ev.Data5;
					Data[5] = ev.Data6;
					Data[6] = ev.Data7;
					
					Integer distance = ev.getDistance();
					Boolean gpsstat = (ev.GPSStat == 1);
					Boolean antstat = (ev.AntStat == 1);
					Integer speeding = ev.getSpeeding();
					Integer duration = ev.getDuration();
					Integer rpm = ev.getRpm();
					Integer brakinglevel = ev.getBreakingLevel();
					
					HBaseTrackingEvent newevent = new HBaseTrackingEvent(
							this.getUnitId(ev.SourceAddress),
							ev.dateTime,
							(Integer)ev.MessageID,
							ev.Longitude,
							ev.Latitude,
							(Integer)ev.ODOmeter,
							toShort(ev.Speed),
							toShort(ev.Direction),
							toShort(distance),
							gpsstat,
							antstat,
							ev.ID,
							speeding,
							duration,
							rpm,
							brakinglevel,
							Data);

					hbaselist.addEvent(newevent);
					localcount++;
					
					evList.add(ev.ID);
				}

				hbaselist.SaveList();
				
				try {
					for (Iterator<Integer> i = evList.iterator(); i.hasNext(); ){
						st.execute("delete from r_mprofevents where \"ID\" = " + i.next());
					}
				}
				catch (Exception e) {
					log.log(Level.DEBUG, "Error removing events from replication table : " + e.getMessage());
					quit();
				}

				rs.close();
		}
		catch(SQLException e) {
			log.log(Level.DEBUG, "Error storing events : " + e.getMessage());
			quit();
		} catch (IOException e) {
			e.printStackTrace();
			quit();
		} catch (Exception e) {
			e.printStackTrace();
			quit();
		}
	}

	private Short toShort(Integer i) {
		if (i == null)
			return 0;
		else
			return (short) (int) i;
	}

}
