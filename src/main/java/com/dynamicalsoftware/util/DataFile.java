package com.dynamicalsoftware.util;

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
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;

public abstract class DataFile {

    public static List<String> extractKeys(String fn) throws IOException {
    	List<String> retVal = new ArrayList<String>();
    	BufferedReader br = new BufferedReader(new FileReader(fn));
    	String line = br.readLine();
    	while  (line != null) {
    		String[] lp = line.split("\t");
    		if (lp.length > 0) {
    			retVal.add(lp[0]);
    		}
    		line = br.readLine();
    	}
    	br.close();
    	Collections.sort(retVal);
    	return retVal;
    }
    
    public static String[] getColumns(String line) throws IOException {
		CSVReader reader = new CSVReader(new InputStreamReader(new ByteArrayInputStream(line.getBytes())));
		String[] retVal = reader.readNext();
		reader.close();
		return retVal;
	}
    	
}