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

/**
 * responsible for populating the star schema based on hadoop map/reduce output files
 * @author glenn
 */
public class LoadStarDB {
	
	/**
	 * connection to the relational database where OLAP will get its data
	 */
	private Connection db = null;
	
	/**
	 * maps table name to last used primary key
	 */
	private Map<String, Integer> lastPrimaryKey = new HashMap<String, Integer>();
	
	/**
	 * list of categories extracted from data
	 */
	private List<String> categories = null;
	
	/**
	 * list of districts extracted from data
	 */
	private List<String> districts = null;
	
	/**
	 * maps a date to the primary key of the corresponding row in the time period table
	 */
	private final java.util.Map<Date, Integer> timeperiodLookup = new HashMap<Date, Integer>();	
	
	/**
	 * formats date for insertion into the relational database
	 */
	private final DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
	
	/**
	 * parses date from the map/reduce job output files
	 */
	private final DateFormat kdf = new SimpleDateFormat("yyyy/MM/dd");

	/**
	 * inserts a row into a table in the relational database
	 * @param table is the name of the table
	 * @param row contains the information to be inserted
	 * @return the primary key of this row
	 * @throws SQLException
	 */
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

	/**
	 * inserts a category row into the database
	 * @param category is the name of the category
	 * @return the primary key for the inserted row
	 * @throws SQLException
	 */
	private int insertCategory(String category) throws SQLException {
		DataRecord dr = new DataRecord();
		dr.put("name", category);
		return insert("category", dr);
	}

	/**
	 * inserts a district row into the database
	 * @param district is the name of the district
	 * @return the primary key for the inserted row
	 * @throws SQLException
	 */
	private int insertDistrict(String district) throws SQLException {
		DataRecord dr = new DataRecord();
		dr.put("name", district);
		return insert("district", dr);
	}
		
	/**
	 * responsible for breaking down a date into year, month, week, and day
	 * @param dr holds the breakdown
	 * @param d is the date to be broken down
	 */
	private void setTimePeriod(DataRecord dr, Date d) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(d);
		dr.put("year", cal.get(Calendar.YEAR));
		dr.put("month", cal.get(Calendar.MONTH));
		dr.put("week", cal.get(Calendar.WEEK_OF_MONTH));
		dr.put("day", cal.get(Calendar.DAY_OF_MONTH));
	}

	/**
	 * inserts a new time period row into the database
	 * @param d the date to be inserted if it has not already done so previously
	 * @return the primary key for this row (new or old)
	 * @throws SQLException
	 */
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
	
	/**
	 * inserts a row into the fact table
	 * @param districtId foreign key into corresponding row of district table
	 * @param categoryId foreign key into corresponding row of category table
	 * @param timeId foreign key into corresponding row of timeperiod table
	 * @param crimes is the total crimes committed in this district of this category at his time
	 * @throws SQLException
	 */
	private void insertFact(int districtId, int categoryId, int timeId, int crimes) throws SQLException {
		DataRecord dr = new DataRecord();
		dr.put("district_id", districtId);
		dr.put("category_id", categoryId);
		dr.put("time_id", timeId);
		dr.put("crimes", crimes);
		insert("fact", dr);
	}
	
	/**
	 * load category and district data from files created during the San Francisco Crime map/reduce job
	 * @param categoryReport fully qualified path and file to bycategory/part-00000
	 * @param districtReport fully qualified path and file to bydistrict/part-00000
	 * @throws IOException
	 * @throws SQLException
	 */
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

	/**
	 * truncate a table so that we know what the next primary key value will be
	 * @param name identifies which table to truncate
	 * @throws SQLException
	 */
	private void truncate(String name) throws SQLException {
		Statement s = db.createStatement();
		s.execute("truncate table ".concat(name));
		s.close();
	}

	/**
	 * truncate all tables in the star schema that this job is to repopulate
	 * @throws SQLException
	 */
	private void reset() throws SQLException {
		truncate("fact");
		truncate("category");
		truncate("district");
		truncate("timeperiod");
	}

	/**
	 * prepare to load the star schema for OLAP
	 * @param categoryReport fully qualified path and file to bycategory/part-00000
	 * @param districtReport fully qualified path and file to bydistrict/part-00000
	 * @param dbhost name of the host where mysql is running
	 * @param dbname name of the database where the star schema has been created
	 * @param dbuser user name with which to authenticate with mysql
	 * @param dbpassword password with which to authenticate with mysql
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException
	 */
	private LoadStarDB(String categoryReport, String districtReport, String dbhost, String dbname, String dbuser, String dbpassword) throws ClassNotFoundException, SQLException, IOException {
		Class.forName("com.mysql.jdbc.Driver");
		String cs = MessageFormat.format("jdbc:mysql://{0}/{1}?user={2}&password={3}&noAccessToProcedureBodies=true", new Object[]{dbhost, dbname, dbuser, dbpassword});
		db = DriverManager.getConnection(cs);
		reset();
		setup(categoryReport, districtReport);
	}
	
	/**
	 * process the SanFranciscoCrimPrepOlap map/reduce job output to populate the timeperiod and fact tables
	 * @param dataFile fullyqualified path and file to star/part-00000
	 * @throws IOException
	 * @throws ParseException
	 */
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
	
	/**
	 * CLI for running this job
	 * @param args
	 */
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

    /**
     * represents a record to be inserted into a table
     * @author glenn
     */
	class DataRecord extends HashMap<String, Object> {

		@Override
		public String toString() {
			StringBuffer retVal = new StringBuffer();
			// generate the field list part of the SQL insert statement
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
			// generate the values part of the SQL insert statement
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