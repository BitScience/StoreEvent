package storeEvent;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.apache.log4j.Logger;

public class Event {
    private static int LAT_LON = 0x00000001;
    private static int GPS_ANT = 0x00000002;
    private static int IN_OUT = 0x00000004;
    private static int ODOMETER = 0x00000008;
    private static int DRIVERKEY = 0x00000010;
    private static int DATAS = 0x00000020;
    private static int SPEED = 0x00000040;
    private static int DIRECTION = 0x00000080;
    private static int DATA_1 = 0x00000100;
    private static int DATA_2 = 0x00000200;
    private static int DATA_3 = 0x00000400;
    private static int DATA_4 = 0x00000800;
    private static int DATA_5 = 0x00001000;
    private static int DATA_6 = 0x00002000;
    private static int DATA_7 = 0x00004000;
    private static int DYNAMIC = 0x00008000;
    private static int DATE_TIME = 0x00020000;
    
	private static Logger log = Logger.getLogger(StoreEvent.class);
    
    private static int LATLON = 1000000;
	
    public static int MES_POS = 1;
    public static int MES_POLL = 6;
    public static int MES_START = 122;
    public static int MES_STOP = 123;
    public static int MES_SPEED = 150;
    public static int MES_EXACC = 153;
    public static int MES_BREAK = 155;
    public static int MES_RECK = 236;
    public static int MES_MIL = 237;
    public static int MES_FAKE_STOP = 300;
    public static int MES_EVAL = 241;
    public static int MES_DEV_PLUGIN = 244;
    public static int MES_DEV_REMOVE = 245;
    public static int MES_BATT = 215;
    
    public int AntStat;
    public int Data1;
    public int Data2;
    public int Data3;
    public int Data4;
    public int Data5;
    public int Data6;
    public int Data7;
    public Timestamp dateTime;
    public String Description;
    public int Direction;
    public int DriverID;
    public int GPSStat;
    public int Information;
    public int InStat;
    public Double Longitude;
    public Double Latitude;
    public int MessageGroup;
    public int MessageID;
    public int ODOmeter;
    public int OutStat;
    public String Registration;
    public int Source;
    public String SourceAddress;
    public int Speed;
    public int VehicleID;
    public int ID;
    public int LeverancierID;
    public Boolean Verwerkt;
    
    private Boolean inUTC;
    
    public Event(ResultSet rs)
    {
    	readFromResultSet(rs);
    }
    
	
	public Event(int id, Connection conn)
	{
		ID = 0;
		
		try {
			
			Statement st = conn.createStatement();
			String s_sql;
			ResultSet rs;

			s_sql = "SELECT * FROM mprofevents WHERE mprofevents.\"ID\" = " + id;
			rs = st.executeQuery(s_sql);
			while (rs.next()) {
				readFromResultSet(rs);
			}
		}
		catch (SQLException e)
		{
			log.error("Error reading event from database: " + ID);
			ID = 0;
		}

	}
	
	private void readFromResultSet(ResultSet rs)
	{
    	try {
			AntStat = rs.getInt(1);
			Data1 = rs.getInt(2);
			Data2 = rs.getInt(3);
			Data3 = rs.getInt(4);
			Data4 = rs.getInt(5);
			Data5 = rs.getInt(6);
			Data6 = rs.getInt(7);
			Data7 = rs.getInt(8);
		    dateTime = rs.getTimestamp(9);
		    Description = rs.getString(10);
		    Direction = rs.getInt(11);
		    DriverID = rs.getInt(12);
		    GPSStat = rs.getInt(13);
		    Information = rs.getInt(14);
		    InStat = rs.getInt(15);
		    Latitude = rs.getDouble(16);
		    Longitude = rs.getDouble(17);
		    MessageGroup = rs.getInt(18);
		    MessageID = rs.getInt(19);
		    ODOmeter = rs.getInt(20);
		    OutStat = rs.getInt(21);
		    Registration = rs.getString(22);
		    Source = rs.getInt(23);
		    SourceAddress = rs.getString(24);
		    Speed = rs.getInt(25);
		    VehicleID = rs.getInt(26);
		    ID = rs.getInt(27);
		    LeverancierID = rs.getInt(28);
		    Verwerkt = rs.getBoolean(29);
		    
		    inUTC = true;
    	}
    	catch (SQLException e) {
    		ID = 0;
    	}
		
	}
/*	
	public void save(Boolean asNew, Connection conn)
	{
		if (ID > 0)
		{
			try {
				Statement st = conn.createStatement();

				StringBuilder sb_insert = new StringBuilder();
				sb_insert.append("INSERT INTO mprofevents VALUES (");
                sb_insert.append(QueryBuilder.insert(AntStat));
                sb_insert.append(QueryBuilder.insert(Data1));
                sb_insert.append(QueryBuilder.insert(Data2));
                sb_insert.append(QueryBuilder.insert(Data3));
                sb_insert.append(QueryBuilder.insert(Data4));
                sb_insert.append(QueryBuilder.insert(Data5));
                sb_insert.append(QueryBuilder.insert(Data6));
                sb_insert.append(QueryBuilder.insert(Data7));
//                if (!inUTC)
//                    sb_insert.append("'" + UTCtoLocalTime(DateTime, Information, dtMax).ToString("s") + "', ");
//                else
                sb_insert.append(QueryBuilder.insert(dateTime));  // "s"
                sb_insert.append(QueryBuilder.insert(Description));
                sb_insert.append(QueryBuilder.insert(Direction));
                sb_insert.append(QueryBuilder.insert(DriverID));
                sb_insert.append(QueryBuilder.insert(GPSStat));
                sb_insert.append(QueryBuilder.insert(Information));
                sb_insert.append(QueryBuilder.insert(InStat));
                sb_insert.append(QueryBuilder.insert(Latitude));
                sb_insert.append(QueryBuilder.insert(Longitude));
                sb_insert.append(QueryBuilder.insert(MessageGroup));
                sb_insert.append(QueryBuilder.insert(MessageID));
                sb_insert.append(QueryBuilder.insert(ODOmeter));
                sb_insert.append(QueryBuilder.insert(OutStat));
                if (Registration != null)
                    sb_insert.append(QueryBuilder.insert(Registration));
                else
                    sb_insert.append("'', ");
                sb_insert.append(QueryBuilder.insert(Source));
                sb_insert.append(QueryBuilder.insert(SourceAddress));
                sb_insert.append(QueryBuilder.insert(Speed));
                if (asNew == true)
                	sb_insert.append(QueryBuilder.insert_last(VehicleID));
                else
                {
                	sb_insert.append(VehicleID + ", ");
                	sb_insert.append(ID + ")");
                }
				
				
				st.execute(sb_insert.toString());

				st.close();
				
			}
			catch (SQLException e)
			{
				ID = 0;
			}
			
		}
	}
	*/
	public void setVerwerkt(Connection conn)
	{
		if (ID > 0)
		{
			try {
				Statement st = conn.createStatement();
				st.execute("UPDATE mprofevents SET \"Verwerkt\" = 't' WHERE mprofevents.\"ID\" = " + ID);

				st.close();
				
			}
			catch (SQLException e)
			{
				log.error("Could not set event Processed: " + ID);
				ID = 0;
			}
		}
		
	}
	
	public void clean_odo()
	{
     if ((Information & ODOMETER) > 0)
      {
          if (ODOmeter == 4294967) // Speciaal geval als unit net begint te werken. Eigenlijk moet ODO op 0 beginnen 
        	  ODOmeter = 0;
      }
      else
    	  ODOmeter = -1;
		
	}
	

	static public void delete(int id, Connection conn)
	{
		try {
			Statement st = conn.createStatement();
			
			st.execute("DELETE FROM mprofevents WHERE mprofevents.\"ID\" = " + id);
			st.close();
		}
		catch(SQLException e)
		{
			log.error("Error deleting event: " + id);
		}
	}
	
	public Integer getDistance() {
		if (MessageID == MES_STOP || MessageID == MES_FAKE_STOP)
			return Data4;  /// afstand in meters als MessID == Trip End
		if (MessageID == MES_POS)
			return Data7; /// tot nog toe afgelegde afstand in meters als MessID == Position
		return 0;
	}
	
	public Integer getSpeeding() {
		if (MessageID == MES_SPEED)
			return Data3;   /// aantal seconden als MessID == Speeding
		return 0;
	}
	
	public Integer getDuration() {
		if (MessageID == MES_STOP || MessageID == MES_FAKE_STOP)
			return Data3;  /// aantal seceonden van de rit als MessID == Trip End
		if (MessageID == MES_POS)
			return Data6;  /// aantal seconden dat de rit nu duurt als MessID == Position
		return Data3;
	}
	
	public Integer getRpm() {
		if (MessageID == MES_POS)
			return Data3; /// Toeren per minuut als MessID == Position
		return 0;
	}
	
	public Integer getBreakingLevel() {
		return Data2;  /// Max waarde, voor elk type bericht. Alleen gevuld voor Excessive events
	}

}
