package net.opentsdb.core;

import java.io.IOException;

import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

import com.stumbleupon.async.Deferred;

public class TrendAnalysis {
	
	private final static HBaseClient client;
	private final Config config;
	private static byte[] table;
	private static final byte[] FAMILY = { 't' };
	
	/**
	 * Creates a trends table in HBase that stores the mean
	 * and standard deviation for each hour of each day of
	 * the week.
	 * @param args
	 * @throws IOException
	 */
	public TrendAnalysis(final HBaseClient client, final Config config){
		this.client = client;
		this.config = config;
		String tableName = "trends";
		table = tableName.getBytes();
		initializeRows();
	}
	
	private static void initializeRows() {
		KeyValue mean = null;
		KeyValue standardDev = null;
		// Add rows for each hour of each day of the week
		int[] weekdays = {1, 2, 3, 4, 5, 6, 7};
		for(int day : weekdays) {
			for(int i = 0; i < 24; i++) {
				String rowName = day + "-" + i; // TODO: add metric
				byte[] row = rowName.getBytes();
				mean = new KeyValue(row, FAMILY, "mean".getBytes(), new byte[0]);
				standardDev = new KeyValue(row, FAMILY, "standard_deviation".getBytes(), new byte[0]);
				PutRequest meanData = new PutRequest(table, mean);
				PutRequest standardDevData = new PutRequest(table, standardDev);
				client.put(meanData);
				client.put(standardDevData);
			}
		}
	}
	
	/**
	 * Alternate constructor
	 * @param config An initialized configuration object
	 */
	public TrendAnalysis(final Config config) {
	    this(new HBaseClient(config.getString("tsd.storage.hbase.zk_quorum"),
	                         config.getString("tsd.storage.hbase.zk_basedir")),
	         config);
	  }
	  
	
	private void updateMetric(){
		
	}
	
	private static void getMetrics() {
		
	}
	
	
}
