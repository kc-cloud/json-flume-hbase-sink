package com.kc.flume.hbase.sink;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Set;
import java.util.Map.Entry;

import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class Test {

	public static void main(String[] args) throws JsonIOException, JsonSyntaxException, FileNotFoundException {
		String myJsonFile = "C:\\temp\\logs-statement.json";
		JsonParser parser = new JsonParser();
		JsonObject jsonObject = parser.parse(new FileReader (myJsonFile)).getAsJsonObject();
        Set<Entry<String, JsonElement>> sets = jsonObject.entrySet();

        String client = jsonObject.get("fields").getAsJsonObject().get("client").getAsString();
        String timeStamp = jsonObject.get("timestamp").getAsString();
        String logType = jsonObject.get("type").getAsString();
        String rowKey = client+"::"+logType+"::"+timeStamp;

        System.out.println("====> "+rowKey);
        for (Entry<String, JsonElement> entry : sets) {
        	if (entry.getKey().startsWith("@")) {
        		continue;
        	}
	        
	        String xcontent = entry.getValue().toString();
	        if (xcontent.startsWith("\"") && xcontent.endsWith("\"")) {
	            xcontent = xcontent.substring(1, xcontent.length() - 1);
	        }
	        System.out.println(entry.getKey()+"==>"+xcontent);
        }
	}
}
