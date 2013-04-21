package com.dynamicalsoftware.hadoop.mapreduce;

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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import au.com.bytecode.opencsv.CSVReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import com.dynamicalsoftware.util.DataFile;

/**
 * base class contains factored out commonality between the various map/reduce jobs
 * @author glenn
 */
public abstract class MapReduceJobBase {

	/**
	 * zero based index into sf crime data where the category column is found
	 */
	protected static final int CATEGORY_COLUMN_INDEX = 1;
	
	/**
	 * zero based index into sf crime data where the day of the week column is found
	 */
	protected static final int DAY_OF_WEEK_COLUMN_INDEX = 3;
	
	/**
	 * zero based index into sf crime data where the date column is found
	 */
	protected static final int DATE_COLUMN_INDEX = 4;
	
	/**
	 * zero based index into sf crime data where the distrct column is found
	 */
	protected static final int DISTRICT_COLUMN_INDEX = 6;

	/**
	 * the date format for dates in the sf crime data file
	 */
	protected static final DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
	
	/**
	 * the date format for dates as stored in the hadoop map/reduce output
	 */
	protected static final DateFormat outputDateFormat = new SimpleDateFormat("yyyy/MM/dd");

	/**
	 * convert the string representation of the date column from the sf crime data to a date type
	 * @param value contains string representation of full date/time stamp
	 * @return date object with time truncated
	 * @throws ParseException
	 */
	protected static Date getDate(String value) throws ParseException {
		Date retVal = null;
		String[] dp = value.split(" ");
		if (dp.length > 0) {
			retVal = df.parse(dp[0]);
		}
		return retVal;
	}
	
}