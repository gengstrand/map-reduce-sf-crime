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
import java.util.Calendar;
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
 * map/reduce job responsible for generating weekly summaries by category and district
 * @author glenn
 */
public class SanFranciscoCrime extends MapReduceJobBase {

	private static Logger log = Logger.getLogger(SanFranciscoCrime.class.getCanonicalName());

	/**
	 * factored out common functionality of all the mapper classes
	 */
	public static abstract class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		/**
		 * zero based index identifies which column in the input data should serve as the key
		 */
		protected int keyID = 0;
		
		/**
		 * zero based index identifies which column in the input data shold serve as the value
		 */
		protected int valueID = 0;

		/**
		 * only the key and the value should be passed through to the intermediate output
		 */
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			try {
				String[] col = DataFile.getColumns(line);
				if (col != null) {
					// make sure there is enough columns in the data
					if (col.length >= (DISTRICT_COLUMN_INDEX + 1)) {
						// filter out the first row which contains the column header names
						if (!"date".equalsIgnoreCase(col[valueID])) {
							Text tk = new Text();
							tk.set(col[keyID]);
							Text tv = new Text();
							tv.set(col[valueID]);
							output.collect(tk, tv);
						}
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
			}
			
		}
		
	}

	/**
	 * mapper for category by day of the week
	 * @author glenn
	 */
	public static class CategoryMapByDotw extends Map {
		public CategoryMapByDotw() {
			keyID = CATEGORY_COLUMN_INDEX;
			valueID = DAY_OF_WEEK_COLUMN_INDEX;
		}
	}

	/**
	 * mapper for district by day of the week
	 * @author glenn
	 */
	public static class DistrictMapByDotw extends Map {
		public DistrictMapByDotw() {
			keyID = DISTRICT_COLUMN_INDEX;
			valueID = DAY_OF_WEEK_COLUMN_INDEX;
		}
	}

	/**
	 * mapper for category by date
	 * @author glenn
	 */
	public static class CategoryMapByDate extends Map {
		public CategoryMapByDate() {
			keyID = CATEGORY_COLUMN_INDEX;
			valueID = DATE_COLUMN_INDEX;
		}
	}

	/**
	 * mapper for district by date
	 * @author glenn
	 */
	public static class DistrictMapByDate extends Map {
		public DistrictMapByDate() {
			keyID = DISTRICT_COLUMN_INDEX;
			valueID = DATE_COLUMN_INDEX;
		}
	}

	/**
	 * responsible for generating the report output based on the intermediate output from the mappers
	 * @author glenn
	 */
	public static class ReduceByWeek extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		private static final long millisecondsInAWeek = 1000l * 60l * 60l * 24l * 7l;
		
		/**
		 * reduces the data (category or district) into weekly totals
		 */
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			List<String> incidents = new ArrayList<String>();
			//compile the individual incident dates into a list
			while (values.hasNext()) {
				incidents.add(values.next().toString());
			}
			if (incidents.size() > 0) {
				// sort that list by date
				Collections.sort(incidents);
				java.util.Map<Integer, Integer> weekSummary = new HashMap<Integer, Integer>();
				for (int i=0; i<16; i++) {
					weekSummary.put(i, 0);
				}
				// aggregate each incident into weekly buckets
				for (String incidentDay : incidents) {
					try {
						Date d = getDate(incidentDay);
						Calendar cal = Calendar.getInstance();
						cal.setTime(d);
						int week = cal.get(Calendar.WEEK_OF_MONTH);
						int month = cal.get(Calendar.MONTH);
						int bucket = (month * 5) + week;
						if (weekSummary.containsKey(bucket)) {
							weekSummary.put(bucket, new Integer(weekSummary.get(bucket).intValue() + 1));
						} else {
							weekSummary.put(bucket, new Integer(1));
						}
					} catch (ParseException pe) {
						log.warning(MessageFormat.format("Invalid date {0}", new Object[]{incidentDay}));
					}
				}
				// generate the output report line
				StringBuffer rpt = new StringBuffer();
				boolean first = true;
				for (int week : weekSummary.keySet()) {
					if (first) {
						first = false;
					} else {
						rpt.append(",");
					}
					rpt.append(new Integer(weekSummary.get(week)).toString());
				}
				String list = rpt.toString();
				Text tv = new Text();
				tv.set(list);
				output.collect(key, tv);
			}
		}
		
	}

	private static void generate(String name, Class mapper, String input, String output) throws IOException {
		JobConf conf = new JobConf(SanFranciscoCrime.class);
		conf.setJobName(name);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(mapper);
		conf.setReducerClass(ReduceByWeek.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		JobClient.runJob(conf);		
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length == 3) {
			generate("category-vs-week", CategoryMapByDate.class, args[0], args[1]);
			generate("district-vs-week", DistrictMapByDate.class, args[0], args[2]);
		} else {
			System.err.println("\nusage: bin/hadoop jar sfcrime.hadoop.mapreduce.jobs-0.0.1-SNAPSHOT.jar SanFranciscoCrime path/to/input/directory path/to/category/report path/to/distripution/repot");
		}
	}
	
}
