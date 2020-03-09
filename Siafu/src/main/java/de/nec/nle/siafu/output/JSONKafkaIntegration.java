/*
 * Copyright NEC Europe Ltd. 2006-2007
 * 
 * This file is part of the context simulator called Siafu.
 * 
 * Siafu is free software; you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 * 
 * Siafu is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package de.nec.nle.siafu.output;


import java.io.BufferedOutputStream;
import java.io.File;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;

import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.json.JSONArray;
import org.json.JSONObject;

import de.nec.nle.siafu.model.Agent;
import de.nec.nle.siafu.model.Overlay;
import de.nec.nle.siafu.model.World;
import de.nec.nle.siafu.types.Publishable;

/**
 * This implementation of <code>SimulatorOutputPrinter</code> prints all the
 * information of an agent, plus the value of the overlays (that is, the whole
 * context set) for each of the users, in a Comma Separated Value file format.
 * <p>
 * Each time the interval given in the config file has passed, a printout is
 * generated. If the history option is set to true, the printout will accumulate
 * the data for all the iterations. Otherwise only the latest iteration will be
 * kept.
 * <p>
 * Note that all the fields in the CSV are common to all the entities, and
 * derived from the agent returned by
 * <code>World.getPeople().iterator().next()</code>. Therefore, you need to make
 * sure this particular user has all the entries in his data fields.
 * 
 * @author Miquel Martin
 * 
 * @version for Thiago Feijó
 * 
 */
public class JSONKafkaIntegration implements SimulatorOutputPrinter {	

	private static final int BUFFER_SIZE = 102400;

	private static final int SECOND_TO_MS_FACTOR = 1000;
	
	private static final String KAFKA_SERVER = "localhost:9092";
	
	private World world;

	private String header;

	private Queue<String> headers;

	private String outputPath;

	private File outputFile;

	private BufferedOutputStream out;

	private boolean keepHistory;

	private int intervalInMillis;

	private long lastPrintoutTime;

	private JSONArray jsonArray = new JSONArray();
	
	public void SimpleProducer(JSONObject object) {
	  
		//Assign topicName to string variable
		String topicName = "Siafu-Office";
		  
		// create instance for properties to access producer configs   
		Properties props = new Properties();
		  
		//Assign localhost id
		props.put("bootstrap.servers", KAFKA_SERVER);
		  
		//Set acknowledgements for producer requests.      
		props.put("acks", "all");
		  
		//If the request fails, the producer can automatically retry,
		props.put("retries", 0);
		  
		//Specify buffer size in config
		props.put("batch.size", 16384);
		  
		//Reduce the no of requests less than 0   
		props.put("linger.ms", 1);
		  
		//The buffer.memory controls the total amount of memory available to the producer for buffering.   
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		      
	    Producer<String, String> producer = new KafkaProducer<String, String>(props);
	      
	    producer.send(new ProducerRecord<String, String>(
	    		topicName, 
	    		object.get("time").toString()+object.get("entityID").toString(),
	    		object.toString()));
		String str = new String();
		

	    System.out.println("Message sent successfully");
	    producer.close();
	}
	
	public JSONKafkaIntegration(final World world, final Configuration config) {
		this.world = world;
		this.outputPath = config.getString("output.csv.path");
		this.keepHistory = config.getBoolean("output.csv.keephistory");
		this.intervalInMillis = SECOND_TO_MS_FACTOR * config.getInt("output.csv.interval");
		
		if (keepHistory) {
			//initializeFile(outputPath);
		}
	}

	private Queue createHeader() {
		header = new String();

		Queue<String> headers = new LinkedList<>(); 
		
		header += "time,";
		header += "entityID,";
		header += "position,";
		header += "atDestination,";
		
		headers.add("time");
		headers.add("entityID");
		headers.add("position");
		headers.add("atDestination");
		
				
		// Person info fields
		String[] infoFields = Agent.getInfoKeys().toArray(new String[0]);

		for (String field : infoFields) {
			header += (field + ",");
			headers.add(field);
		}

		// Overlays
		for (String overlay : world.getOverlays().keySet()) {
			header += (overlay + ",");
			headers.add(overlay);
		}
		// Adding string header to CSV 
		// Remove last comma
		this.header = header.substring(0, header.lastIndexOf(","));
		
		// Returning headers like a queue
		return headers;
	}

	public void notifyIterationConcluded() {
		long lastPrintoutAge = world.getTime().getTimeInMillis()
				- lastPrintoutTime;
		if (lastPrintoutAge > intervalInMillis) {
			lastPrintoutTime = world.getTime().getTimeInMillis();

			if (!keepHistory) {
				initializeFile(outputPath + ".tmp");
			}

			for (Agent agent : world.getPeople()) {
				this.headers = createHeader();
				createJSONObject(this.headers, agent);
			}

			if (!keepHistory) {
				cleanup();
				outputFile.renameTo(new File(outputPath));
			}
		}
	}

	private void initializeFile(final String filePath) {
		try {
			outputFile = new File(filePath);
			//PrintStream outStream = new PrintStream(outputFile);
			//out = new BufferedOutputStream(outStream, BUFFER_SIZE);
		} catch (Exception e) {
			throw new RuntimeException("Can't create the output file: "
					+ filePath, e);
		}
	}

	public void saveJSONFile(){
		
	}
	
	private void createJSONList(JSONObject object) {
		this.jsonArray.put(object);
	}
	
	private void createJSONObject(Queue headers, final Agent agent) {
	
		JSONObject object = new JSONObject();
		
		String insertion = new String();
		
		
		object.put((String) headers.remove(), world.getTime().getTimeInMillis());
		
		object.put((String) headers.remove(), agent.getName()); 
		
		object.put((String) headers.remove(), agent.getPos().toString());
		
		object.put((String) headers.remove(), agent.isAtDestination());
		
		for (Publishable info : agent.getInfoValues()) {
			object.put((String) headers.remove(), info.flatten()); 
		}
		
		for (Overlay overlay : world.getOverlays().values()) {
			//System.out.println(overlay.getValue(agent.getPos()).toString());
			object.put((String) headers.remove(), overlay.getValue(agent.getPos()));
		}
		
		//createJSONList(object);
		SimpleProducer(object);
	}

	public void cleanup() {
		/**
		try {
			out.flush();
			out.close();
		} catch (IOException e) {
			throw new RuntimeException("Can't close the output file", e);
		}
		*/
	}

}
