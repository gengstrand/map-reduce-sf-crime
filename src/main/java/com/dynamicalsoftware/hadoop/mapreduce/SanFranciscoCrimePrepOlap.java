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

import java.io.IOException;
import java.text.ParseException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.dynamicalsoftware.util.DataFile;

/**
 * map/reduce job responsible for generating the data used to load time period and fact tables in the OLAP db
 * @author glenn
 */
public class SanFranciscoCrimePrepOlap extends MapReduceJobBase {

	private static Logger log = Logger.getLogger(SanFranciscoCrimePrepOlap.class.getCanonicalName());

	/**
	 * list of all categories found in the input data
	 */
	private static List<String> categories = null;
	
	/**
	 * list of all districts found in the input data
	 */
	private static List<String> districts = null;
	
	/**
	 * maps category name to index od
	 */
	private static final java.util.Map<String, Integer> categoryLookup = new HashMap<String, Integer>();
	
	/**
	 * maps district name to index id
	 */
	private static final java.util.Map<String, Integer> districtLookup = new HashMap<String, Integer>();	
	
	/**
	 * base class for any OLAP related mappers
	 * @author glenn
	 */
	public static abstract class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		protected int keyID = 0;
		protected int valueID = 0;
		protected int value2ID = 0;
		
		/**
		 * needed to normalize the key in order to make it friendly for the default hadoop combiner
		 * @param value contains the non-normalized key
		 * @return the normalized key
		 * @throws ParseException
		 */
		protected abstract String formatKey(String value) throws ParseException;
		
		/**
		 * extracts out the key and two values to be collecting into the intermediate data
		 */
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			try {
				String[] col = DataFile.getColumns(line);
				if (col != null) {
					if (col.length >= (DISTRICT_COLUMN_INDEX + 1)) {
						Text tk = new Text();
						tk.set(formatKey(col[keyID]));
						Text tv = new Text();
						StringBuffer sv = new StringBuffer();
						sv.append("\"");
						sv.append(col[valueID]);
						sv.append("\"");
						sv.append(",");
						sv.append("\"");
						sv.append(col[value2ID]);
						sv.append("\"");
						tv.set(sv.toString());
						output.collect(tk, tv);
					} else {
						log.warning(MessageFormat.format("Data {0} did not parse into columns.", new Object[]{line}));
					}
				} else {
					log.warning(MessageFormat.format("Data {0} did not parse into columns.", new Object[]{line}));
				}
			} catch (NumberFormatException nfe) {
				log.log(Level.WARNING, MessageFormat.format("Expected {0} to be a number.\n", new Object[]{line}), nfe);
			} catch (IOException e) {
				log.log(Level.WARNING, MessageFormat.format("Cannot parse {0} into columns.\n", new Object[]{line}), e);
			} catch (ParseException e) {
				log.log(Level.WARNING, MessageFormat.format("Expected {0} to be a date but it was not.\n", new Object[]{line}), e);
			}
			
		}
		
	}
	
	/**
	 * map raw input by collecting only the date as the key and the district and category as the values
	 * @author glenn
	 */
	public static class DateMapByCategoryAndDistrict extends Map {
		public DateMapByCategoryAndDistrict() {
			keyID = DATE_COLUMN_INDEX;
			valueID = DISTRICT_COLUMN_INDEX;
			value2ID = CATEGORY_COLUMN_INDEX;
		}

		@Override
		protected String formatKey(String value) throws ParseException {
			return outputDateFormat.format(getDate(value));
		}
	}
	
	/**
	 * responsible for generating the report output based on the intermediate output from the mappers
	 * @author glenn
	 */
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		/**
		 * non zero counts of crimes by date of every combination of district and category
		 * @author glenn
		 */
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			// allocate and initialize 2D array of category by district
			int[][] crimes = new int[categories.size()][districts.size()];
			for (int i = 0; i < categories.size(); i++) {
				for (int j = 0; j < districts.size(); j++) {
					crimes[i][j] = 0;
				}
			}
			// create the heat map of crimes in the category/district array
			while (values.hasNext()) {
				String crime = values.next().toString();
				String[] cols = DataFile.getColumns(crime);
				if (cols.length == 2) {
					if (categoryLookup.containsKey(cols[1])) {
						if (districtLookup.containsKey(cols[0])) {
							int cat = categoryLookup.get(cols[1]);
							int dist = districtLookup.get(cols[0]);
							crimes[cat][dist]++;
						} else {
							log.warning(MessageFormat.format("District {0} not found.", new Object[]{cols[0]}));
						}
					} else {
						log.warning(MessageFormat.format("Category {0} not found.", new Object[]{cols[1]}));
					}
				} else {
					log.warning(MessageFormat.format("Input {0} was in unexpected format", new Object[]{crime}));
				}
			}
			// serialize the non zero entries as a triplet of category index, district index, and total crimes per day
			for (int i = 0; i < categories.size(); i++) {
				for (int j = 0; j < districts.size(); j++) {
					if (crimes[i][j] > 0) {
						StringBuffer sv = new StringBuffer();
						sv.append(new Integer(i).toString());
						sv.append(",");
						sv.append(new Integer(j).toString());
						sv.append(",");
						sv.append(new Integer(crimes[i][j]));
						Text tv = new Text();
						tv.set(sv.toString());
						output.collect(key, tv);
					}
				}
			}
		}
	}
	
	/**
	 * load the previously generated category and district reports in order to get the list of categories, districts, and their look up mappings
	 * @param categoryReport contains the fully qualified path and file to the bycategory/part-00000 file generated by the SanFranciscoCrime job
	 * @param districtReport contains the fully qualified path and file to the bydistrict/part-00000 file generated by the SanFranciscoCrime job
	 * @throws IOException
	 */
	private static void  setup(String categoryReport, String districtReport) throws IOException {
		categories = DataFile.extractKeys(categoryReport);
		districts = DataFile.extractKeys(districtReport);
		int i = 0;
		for (String category : categories) {
			categoryLookup.put(category, i++);
		}
		i = 0;
		for (String district : districts) {
			districtLookup.put(district, i++);
		}
	}
	
	/**
	 * sets up and runs the hadoop map/reduce job itself
	 * @param name contains the name of the job itself
	 * @param mapper identified which mapper class to use
	 * @param input is the fully qualified path to the raw crime data
	 * @param output is the fully qualified path to where the generated data should reside
	 * @throws IOException
	 */
	private static void generate(String name, Class mapper, String input, String output) throws IOException {
		JobConf conf = new JobConf(SanFranciscoCrimePrepOlap.class);
		conf.setJobName(name);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(mapper);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		JobClient.runJob(conf);		
	}

	/**
	 * enforce proper CLI usage then drive the overall process
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length == 4) {
			setup(args[0], args[1]);
			generate("daily-activity", DateMapByCategoryAndDistrict.class, args[2], args[3]);
		} else {
			System.err.println("\nusage: bin/hadoop jar sfcrime.hadoop.mapreduce.jobs-0.0.1-SNAPSHOT.jar SanFranciscoCrimePrepOlap path/to/category/report path/to/district/report path/to/input/data path/to/output/data");
		}
	}
	
}
