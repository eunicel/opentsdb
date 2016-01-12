package net.opentsdb.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
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
	private static Set<String> metricsAndTags;
	private static HashMap<String, long[]> allStats;
	
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
		
		// a set of metrics-tags
		metricsAndTags = new HashSet<String>();
		
		// mapping of metrics-tags-day-time to an
		// array of count, mean, and standard deviation
		allStats = new HashMap<String, long[]>(); 
		
		String tableName = "trends";
		table = tableName.getBytes();
	}
	
	/**
	 * Adds all the rows needed to store trends for this metric.
	 * Creates a row for each hour of each day of the week for this metric.
	 * @param metric
	 */
	private static void addNewMetric(String newMetricAndTags, long timestamp, long value, Map<String, String> tags) {
		log.info("start initializing rows for metric " + newMetricAndTags);
		
		metricsAndTags.add(newMetricAndTags);
		
		KeyValue mean = null;
		KeyValue standardDev = null;
		
		// Add rows for each hour of each day of the week
		int[] weekdays = {1, 2, 3, 4, 5, 6, 7};
		for(int day : weekdays) {
			for(int i = 0; i < 24; i++) {
				log.info("row " + day + ", " + i);
				String rowName = newMetricAndTags + "-" + day + "-" + i;
				
				// Store in memory
				long[] stats = {1, value, 0}; // count, mean, standard deviation
				allStats.put(rowName, stats);
				
				// Store into HBase
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
	
	/**
	 * Adds a new point, updates the count, mean, and standard
	 * deviation if it exists. Otherwise, create new rows for
	 * the metric and initialize count, mean, and
	 * standard deviation according to the newly added point.
	 * @param metric
	 * @param timestamp
	 * @param value
	 * @param tags
	 */
	public void addPoint(String metric,
			long timestamp, long value, Map<String, String> tags) {
		log.info("trendAnalysis adding point!!!!!!!!!!!!!! !!!!!!!!");
		ArrayList<String> tagsList = new ArrayList<String>(tags.keySet());
		Collections.sort(tagsList);
		String newMetricAndTags = metric;
		for(String tag : tagsList) {
			newMetricAndTags = newMetricAndTags + "-" + tag;
		}
		if(metricsAndTags.contains(newMetricAndTags)) {
			String rowName = newMetricAndTags + getDay(timestamp) + getHour(timestamp);
			long[] stats = allStats.get(rowName);
			// update stats in memory
			allStats.put(rowName, updateStats(stats, value));
		} else {
			addNewMetric(newMetricAndTags, timestamp, value, tags);
		}
	}
	
	/**
	 * Updates the stats for a particular row with the new value.
	 * Updates standard deviation according
	 * https://en.wikipedia.org/wiki/Standard_deviation#Population-based_statistics
	 * @param stats An array of count, mean, and standard deviation
	 * @param value The new value to add
	 * @return
	 */
	private long[] updateStats(long[] stats, long value) {
		long count = stats[0];
		long mean = stats[1];
		long stdev = stats[2];
		
		stats[1] = ((mean * count) + value) / (count + 1); // update mean
		stats[0] += 1; // update count
		stats[2] = (long) Math.sqrt((count * Math.pow(stdev, 2) / (count + 1)
				+ (count / Math.pow((count + 1), 2))
				* Math.pow((mean - value), 2))); // update standard deviation
		return stats;
	}
	
	/**
	 * Gets the day of the week given the epoch timestamp.
	 * Monday = 1, Tuesday = 2, etc.
	 * @param timestamp
	 */
	private int getDay(long timestamp) {
		int day = 0;
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date(timestamp * 1000));
		int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
		if(dayOfWeek == Calendar.MONDAY){
			day = 1;
		} else if (dayOfWeek == Calendar.TUESDAY) {
			day = 2;
		} else if (dayOfWeek == Calendar.WEDNESDAY) {
			day = 3;
		} else if (dayOfWeek == Calendar.THURSDAY) {
			day = 4;
		} else if (dayOfWeek == Calendar.FRIDAY) {
			day = 5;
		} else if (dayOfWeek == Calendar.SATURDAY) {
			day = 6;
		} else if (dayOfWeek == Calendar.SUNDAY) {
			day = 7;
		} else {
			log.info("ERROR: Not a day of the week");
		}
		return day;
	}
	
	/**
	 * Gets the hour of the day given the epoch timestamp.
	 * E.g., at 10:04:15.250 PM getHour will return 22.
	 * @param timestamp
	 * @return
	 */
	private int getHour(long timestamp) {
		int hour;
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date(timestamp * 1000));
		return cal.get(Calendar.HOUR);
	}
	
	private void addToHBase() {
		
	}
	
}
