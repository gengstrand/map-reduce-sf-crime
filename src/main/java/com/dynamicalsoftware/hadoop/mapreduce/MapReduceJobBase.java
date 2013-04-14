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

public abstract class MapReduceJobBase {

	protected static final int CATEGORY_COLUMN_INDEX = 1;
	protected static final int DAY_OF_WEEK_COLUMN_INDEX = 3;
	protected static final int DATE_COLUMN_INDEX = 4;
	protected static final int DISTRICT_COLUMN_INDEX = 6;

	protected static final DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
	protected static final DateFormat outputDateFormat = new SimpleDateFormat("yyyy/MM/dd");

	protected static String[] getColumns(String line) throws IOException {
		return DataFile.getColumns(line);
	}
	
	protected static Date getDate(String value) throws ParseException {
		Date retVal = null;
		String[] dp = value.split(" ");
		if (dp.length > 0) {
			retVal = df.parse(dp[0]);
		}
		return retVal;
	}
	
}