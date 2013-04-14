package com.dynamicalsoftware.olap.etl;

/*
Copyright 2013 Dynamical Software, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dynamicalsoftware.util.DataFile;

public class LoadStarDB {
	private Connection db = null;
	private Map<String, Integer> lastPrimaryKey = new HashMap<String, Integer>();
	private List<String> categories = null;
	private List<String> districts = null;
	private final java.util.Map<Date, Integer> timeperiodLookup = new HashMap<Date, Integer>();	
	private final DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
	private final DateFormat kdf = new SimpleDateFormat("yyyy/MM/dd");
	
	private static final long millisecondsInADay = 1000l * 60l * 60l * 24l;
	private static final long millisecondsInAWeek = millisecondsInADay * 7l;
	private static final long millisecondsInAMonth = millisecondsInAWeek * 4l;
	private static final long millisecondsInAYear = millisecondsInAMonth * 12l;
	
	private int insert(String table, DataRecord row) throws SQLException {
		int retVal = 0;
		Statement s = db.createStatement();
		StringBuffer sql = new StringBuffer();
		sql.append("insert into ");
		sql.append(table);
		sql.append(" ");
		sql.append(row.toString());
		s.execute(sql.toString());
		if (lastPrimaryKey.containsKey(table)) {
			retVal = lastPrimaryKey.get(table) + 1;
			lastPrimaryKey.put(table, retVal);
		} else {
			lastPrimaryKey.put(table, 1);
			retVal = 1;
		}
		return retVal;
	}

	private int insertCategory(String category) throws SQLException {
		DataRecord dr = new DataRecord();
		dr.put("name", category);
		return insert("category", dr);
	}

	private int insertDistrict(String district) throws SQLException {
		DataRecord dr = new DataRecord();
		dr.put("name", district);
		return insert("district", dr);
	}
		
	private void setTimePeriod(DataRecord dr, Date d) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(d);
		dr.put("year", cal.get(Calendar.YEAR));
		dr.put("month", cal.get(Calendar.MONTH));
		dr.put("week", cal.get(Calendar.WEEK_OF_MONTH));
		dr.put("day", cal.get(Calendar.DAY_OF_MONTH));
	}
	
	private int insertTimePeriod(Date d) throws SQLException {
		int retVal = 0;
		if (timeperiodLookup.containsKey(d)) {
			retVal = timeperiodLookup.get(d);
		} else {
			DataRecord dr = new DataRecord();
			setTimePeriod(dr, d);
			retVal = insert("timeperiod", dr);
			timeperiodLookup.put(d, retVal);
		}
		return retVal;
	}
	
	private void insertFact(int districtId, int categoryId, int timeId, int crimes) throws SQLException {
		DataRecord dr = new DataRecord();
		dr.put("district_id", districtId);
		dr.put("category_id", categoryId);
		dr.put("time_id", timeId);
		dr.put("crimes", crimes);
		insert("fact", dr);
	}
	
	private void setup(String categoryReport, String districtReport) throws IOException, SQLException {
		categories = DataFile.extractKeys(categoryReport);
		districts = DataFile.extractKeys(districtReport);
		for (String category : categories) {
			insertCategory(category);
		}
		for (String district : districts) {
			insertDistrict(district);
		}
	}

	private void truncate(String name) throws SQLException {
		Statement s = db.createStatement();
		s.execute("truncate table ".concat(name));
		s.close();
	}
	
	private void reset() throws SQLException {
		truncate("fact");
		truncate("category");
		truncate("district");
		truncate("timeperiod");
	}
	
	private LoadStarDB(String categoryReport, String districtReport, String dbhost, String dbname, String dbuser, String dbpassword) throws ClassNotFoundException, SQLException, IOException {
		Class.forName("com.mysql.jdbc.Driver");
		String cs = MessageFormat.format("jdbc:mysql://{0}/{1}?user={2}&password={3}&noAccessToProcedureBodies=true", new Object[]{dbhost, dbname, dbuser, dbpassword});
		db = DriverManager.getConnection(cs);
		reset();
		setup(categoryReport, districtReport);
	}
	
	private void processData(String dataFile) throws IOException, ParseException {
    	BufferedReader br = new BufferedReader(new FileReader(dataFile));
    	String line = br.readLine();
    	while  (line != null) {
    		String[] lp = line.split("\t");
    		if (lp.length > 0) {
    			Date d = kdf.parse(lp[0]);
    			String[] data = DataFile.getColumns(lp[1]);
    			if (data.length == 3) {
	    			try {
	    				int categoryId = Integer.parseInt(data[0]) + 1;
	    				int districtId = Integer.parseInt(data[1]) + 1;
	    				int crimes = Integer.parseInt(data[2]);
	    				int timeId = insertTimePeriod(d);
	    				insertFact(districtId, categoryId, timeId, crimes);
	    			} catch (NumberFormatException nfe) {
	    				System.err.println("invalid data: " + line);
	    			} catch (SQLException e) {
						e.printStackTrace();
					}
    			} else {
    				System.err.println("invalid data: " + line);
    			}
    		}
    		line = br.readLine();
    	}
    	br.close();
	}
	
    public static void main(String[] args) {
    	if (args.length == 7) {
    		try {
				LoadStarDB m = new LoadStarDB(args[0], args[1], args[3], args[4], args[5], args[6]);
				m.processData(args[2]);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
    	} else {
    		System.err.println("\nusage: java -jar sfcrime.hadoop.mapreduce.jobs-0.0.1-SNAPSHOT.jar com.dynamicalsoftware.olap.etl.LoadStarDB path/to/category/report path/to/district/report path/to/star/data dbhost dbname dbuser dbpassword\n");
    	}
    }
	
	class DataRecord extends HashMap<String, Object> {

		@Override
		public String toString() {
			StringBuffer retVal = new StringBuffer();
			retVal.append("(");
			boolean first = true;
			for (String key : keySet()) {
				if (first) {
					first = false;
				} else {
					retVal.append(",");
				}
				retVal.append(key);
			}
			retVal.append(") values (");
			first = true;
			for (String key : keySet()) {
				Object o = get(key);
				if (first) {
					first = false;
				} else {
					retVal.append(",");
				}
				if (o instanceof Long) {
					retVal.append(((Long)o).toString());
				} else if (o instanceof Integer) {
					retVal.append(((Integer)o).toString());
				} else if (o instanceof Date) {
					Date d = (Date)o;
					retVal.append("'");
					retVal.append(df.format(d));
					retVal.append("'");
				} else if (o instanceof String) {
					retVal.append("'");
					retVal.append(o.toString());
					retVal.append("'");
				}
			}
			retVal.append(")");
			return retVal.toString();
		}
		
	}
}