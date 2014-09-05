package storeEvent;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.log4j.Level;
import bigdata.HBaseTrackingEvent;
import bigdata.HBaseTrackingEventList;

public class StoreProcessor extends Processor {
	
	public int id;
	public String hbase_ip;
	public String hbase_table;
	
	private HashMap<String, Integer> hmUnits;
	
	@Override
	void init() {
		hmUnits = new HashMap<String, Integer>();
		loadUnits();
	}
	
	@Override	
	String getDesc() {
		return "StoreProcessor";
	}
	
	public StoreProcessor(Connection c, int r, int l) {
		super(c,r,l);
	}
	
	private void loadUnits() {
		
		try {
		
			Statement st1 = conn.createStatement();

			ResultSet idunit = st1.executeQuery("SELECT * FROM nazzaunit"); // Select "SourceAddress", nazzaid FROM nazzaunit;
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
	void proces() {
		
		try {

				localcount = 0;
				ResultSet rs = st.executeQuery("SELECT * FROM mprofevents where \"ID\" >= " + id + " and \"ID\" < " + (id + limit) + " and \"DateTime\" > '2012-10-28 03:00:00'");

				HBaseTrackingEventList hbaselist = new HBaseTrackingEventList(hbase_ip, hbase_table);
						
				while (rs.next()) {

					Event ev = new Event(rs);
					
					if (ev.Longitude > 48.0 && ev.Longitude < 52.5 && ev.Latitude < ev.Longitude) {  // is iets aan de hand. Lat en Long zijn omgedraaid
						Double lat_tmp = ev.Latitude;
						ev.Latitude = ev.Longitude;
						ev.Longitude = lat_tmp;
					}
					
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
					
					if (hmUnits.get(ev.SourceAddress) == null) {
						log.log(Level.DEBUG, "Error reading unit. The ID " + ev.SourceAddress + " does not exist.");
						stop = true;
						break;
					}
					else {

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
					}
				}

				hbaselist.SaveList();

				id += localcount;
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
	
	public void setID(int i) {
		id = i;
		count = i;
	}

}
