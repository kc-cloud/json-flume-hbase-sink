package com.kc.flume.hbase.sink;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


/**
 * A Serializer class that converts the JSON document into column values and send to HBase table
 * 
 * @author kc-cloud
 */
public class JsonHbaseEventSerializer implements HbaseEventSerializer {

	public static void main(String[] str) {}
	
    private static final String CHARSET_CONFIG = "charset";
    private static final String CHARSET_DEFAULT = "UTF-8";

    protected static final AtomicLong nonce = new AtomicLong(0);

    protected byte[] columnFactory;
    private byte[] payload;
    private Map<String, String> headers;
    private Charset charset;

    public void configure(Context context) {
        charset = Charset.forName(context.getString(CHARSET_CONFIG, CHARSET_DEFAULT));
    }

    public void configure(ComponentConfiguration conf) {
    	
    }

    public void initialize(Event event, byte[] columnFamily) {
        this.headers = event.getHeaders();
        this.payload = event.getBody();
        this.columnFactory = columnFamily;
    }

    public List<Row> getActions() throws FlumeException {
        List<Row> actions = Lists.newArrayList();

        try {

            JsonElement element = new JsonParser().parse(new String(payload));

            if (element.isJsonNull() || !element.isJsonObject()) {
                return actions;
            }

            JsonObject jsonObject = element.getAsJsonObject();            
            Set<Entry<String, JsonElement>> sets = jsonObject.entrySet();
            sets.remove(headers);

            String client = jsonObject.get("fields").getAsJsonObject().get("client").getAsString();
            
            String logType = "";
            String hostname = "";
            String timeStamp = jsonObject.get("timestamp").getAsString();
            try {
            	logType = jsonObject.get("type").getAsString();
            } catch (Exception exp) {
            	// ignore if element is missing
            }
            try {
            	hostname = jsonObject.get("hostname").getAsString();
            } catch (Exception exp) {
            	// ignore if element is missing
            }
            String rowKey = client+"::"+hostname+"::"+logType+"::"+timeStamp;
            
            Put put = new Put(rowKey.getBytes());
            for (Entry<String, JsonElement> entry : sets) {
            	if (entry.getKey().startsWith("@")) {
            		continue;
            	}
    	        
    	        String xcontent = entry.getValue().toString();
    	        if (xcontent.startsWith("\"") && xcontent.endsWith("\"")) {
    	            xcontent = xcontent.substring(1, xcontent.length() - 1);
    	        }
    	        put.addColumn(columnFactory, entry.getKey().getBytes(charset), xcontent.getBytes(charset));
            }
            actions.add(put);
        } catch (Exception e) {
        	e.printStackTrace();
            throw new FlumeException(e.getMessage(), e);
        }
        return actions;
    }

    public List<Increment> getIncrements() {
        return Lists.newArrayList();
    }

    public void close() {
    }
}
