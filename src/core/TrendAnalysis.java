package net.opentsdb.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import org.slf4j.Logger;

public class TrendAnalysis {
	
	private static HBaseClient client;
	private final Config config;
	private static byte[] table;
	private static final byte[] FAMILY = { 't' };
	static Logger log = LoggerFactory.getLogger(TrendAnalysis.class);
	private Set<String> metrics;
	
	/**
	 * Creates a trends table in HBase that stores the mean
	 * and standard deviation for each hour of each day of
	 * the week.
	 * @param args
	 * @throws IOException
	 */
	public TrendAnalysis(HBaseClient client, final Config config){
		log.info("in TrendAnalysis constructor");
		this.client = client;
		this.config = config;
		metrics = new HashSet<String>();
		String tableName = "trends";
		table = tableName.getBytes();
	}
	
	/**
	 * Adds all the rows needed to store trends for this metric.
	 * Creates a row for each hour of each day of the week for this metric.
	 * @param metric
	 */
	private static void addMetric(String metric) {
		log.info("start initializing rows for metric " + metric);
		KeyValue mean = null;
		KeyValue standardDev = null;
		// Add rows for each hour of each day of the week
		int[] weekdays = {1, 2, 3, 4, 5, 6, 7};
		for(int day : weekdays) {
			for(int i = 0; i < 24; i++) {
				log.info("row " + day + ", " + i);
				String rowName = metric + "-" + day + "-" + i;
				byte[] row = rowName.getBytes();
				mean = new KeyValue(row, FAMILY, "mean".getBytes(), new byte[0]);
				standardDev = new KeyValue(row, FAMILY, "standard_deviation".getBytes(), new byte[0]);
				PutRequest meanData = new PutRequest(table, mean);
				PutRequest standardDevData = new PutRequest(table, standardDev);
				client.put(meanData);
				client.put(standardDevData);
			}
		}
		try {
			client.flush();
			log.info("flushed");
		} catch (Exception e) {
			e.printStackTrace();
		}
		log.info("done initializing rows");
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
	
	public void addPoint(String metric, long timestamp, long value, Map<String, String> tags) {
		log.info("trendAnalysis adding point!!!!!!!!!!!!!! !!!!!!!!");
		ArrayList<String> tagsList = new ArrayList<String>(tags.keySet());
		Collections.sort(tagsList);
		String metricAndTags = metric;
		for(String tag : tagsList) {
			metricAndTags = metricAndTags + "-" + tag;
		}
		if(metrics.contains(metricAndTags)) {
			// update mean and standard deviation
		} else {
			metrics.add(metricAndTags);
			addMetric(metricAndTags);
		}
	}
	
	
}
