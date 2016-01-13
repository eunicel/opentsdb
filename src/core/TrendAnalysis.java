package net.opentsdb.core;

import java.io.IOException;
import java.nio.ByteBuffer;
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

import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import org.slf4j.Logger;

public class TrendAnalysis {
	
	private static HBaseClient client;
	private final Config config;
	
	private static byte[] table = "trends".getBytes();
	private static final byte[] FAMILY = { 't' };
	private static HashMap<String, long[]> allStats;
	
	static Logger log = LoggerFactory.getLogger(TrendAnalysis.class);

	
	/**
	 * Creates a trends table in HBase that stores the mean
	 * and standard deviation for each hour of each day of
	 * @param client
	 * @param config
	 */
	public TrendAnalysis(HBaseClient client, final Config config){
		log.info("in TrendAnalysis constructor");
		this.client = client;
		this.config = config;
		
		// mapping of metrics-tags-day-time to an
		// array of count, mean, and standard deviation
		allStats = new HashMap<String, long[]>(); 
	}
	
	/**
	 * Alternate constructor
	 * @param config An initialized configuration object
	 */
	public TrendAnalysis(final Config config) {
	    this(new HBaseClient(config.getString("tsd.storage.hbase.zk_quorum"),
	                         config.getString("tsd.storage.hbase.zk_basedir")), config);
	  }

	/**
	 * Creates a new row for this new data point in HBase
	 * and initializes the stats based on its value.
	 * @param rowName
	 * @param value
	 */
	private static void createNewRowInHBase(String rowName, long value) {
		log.info("start creating row " + rowName);
		
		// initial values for count, mean, and standard deviation
		long[] initialData = {1, value, 0};
		addDataToHBase(rowName, initialData);
		
		try {
			client.flush();
			log.info("flushed");
		} catch (Exception e) {
			e.printStackTrace();
		}
		log.info("done initializing rows");
	}
	
	private static void addDataToHBase(String rowName, long[] stats) {
		byte[] row = rowName.getBytes();

		byte countBytes[] = new byte[8];
		ByteBuffer countBuf = ByteBuffer.wrap(countBytes);
		countBuf.putLong(stats[0]);
		KeyValue countKv =
				new KeyValue(row, FAMILY, "count".getBytes(), countBytes);
		
		byte meanBytes[] = new byte[8];
		ByteBuffer meanBuf = ByteBuffer.wrap(meanBytes);
		meanBuf.putLong(stats[1]);
		KeyValue meanKv =
				new KeyValue(row, FAMILY, "mean".getBytes(), meanBytes);
		
		byte stdevBytes[] = new byte[8];
		ByteBuffer stdevBuf = ByteBuffer.wrap(stdevBytes);
		stdevBuf.putLong(stats[2]);
		KeyValue standardDevKv =
				new KeyValue(row, FAMILY, "standard_deviation".getBytes(), stdevBytes);
		
		PutRequest countData = new PutRequest(table, countKv);
		PutRequest meanData = new PutRequest(table, meanKv);
		PutRequest standardDevData = new PutRequest(table, standardDevKv);
		
		client.put(countData);
		client.put(meanData);
		client.put(standardDevData);
	}
	
	/**
	 * Updates the given row in Hbase with the given value.
	 * @param rowName
	 * @param value
	 */
	private void updateRowInHBase(String rowName, long value) {
		long count = getStatFromHBase(rowName, "count");
		long mean = getStatFromHBase(rowName, "mean");
		long stdev = getStatFromHBase(rowName, "standard_deviation");
		long[] oldStats = {count, mean, stdev};
		log.info("mean = " + mean);
		log.info("count = " + count);
		log.info("stdev = " + stdev);
		long[] newStats = updateStats(oldStats, value);
		addDataToHBase(rowName, newStats);
		try {
			client.flush();
			log.info("flushed");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Given the row, get the requested statistic.
	 * @param rowName Key of the row
	 * @param stat Statistic to return (count, mean, or standard deviation)
	 * @return oldStat Requested statistic of the given row
	 */
	private long getStatFromHBase(String rowName, String stat) {
		long oldStat = 0;
		try {
			log.info("getting " + stat + " !!!!!!!!!!!!!!!!");
			GetRequest get = new GetRequest(table, rowName.getBytes(), FAMILY, stat.getBytes());
			KeyValue kv = client.get(get).join().get(0);
			byte[] bytes = kv.value();
			for (int i = 0; i < bytes.length; i++) {
				oldStat = (oldStat << 8) + (bytes[i] & 0xff); // converts from byte[] to long
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.info("ERROR getting " + stat + " from HBase");
		}
		return oldStat;
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
		
		// Create ordered list of tag keys and values
		ArrayList<String> tagsList = new ArrayList<String>(tags.keySet());
		Collections.sort(tagsList);
		String tagsAndValues = "";
		for(String tag : tagsList) {
			tagsAndValues = tagsAndValues + "-" + tag + "=" + tags.get(tag);
		}
		
		// Construct key in allStats table
		String rowName = metric + tagsAndValues + "-" + getDay(timestamp) + "-" + getHour(timestamp);
		
		if(allStats.containsKey(rowName)) { // update stats in memory
			long[] stats = allStats.get(rowName);
			log.info("addPoint - updating HBase");
			allStats.put(rowName, updateStats(stats, value));
			updateRowInHBase(rowName, value);
		} else { // store stats in memory
			long[] stats = {1, value, 0}; // count, mean, standard deviation
			allStats.put(rowName, stats);
			log.info("addPoint - creating row in HBase");
			createNewRowInHBase(rowName, value);
		}
	}
	
	/**
	 * Updates the stats for a particular row with the new value.
	 * Updates standard deviation according
	 * https://en.wikipedia.org/wiki/Standard_deviation#Population-based_statistics
	 * @param stats An array of count, mean, and standard deviation
	 * @param value The new value to add
	 * @return stats The updated coutn, mean, and standard deviation
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
	 * Sunday = 1, Monday = 2, Tuesday = 3, etc.
	 * @param timestamp
	 */
	private static int getDay(long timestamp) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date(timestamp * 1000));
		return cal.get(Calendar.DAY_OF_WEEK);
	}
	
	/**
	 * Gets the hour of the day given the epoch timestamp.
	 * E.g., at 10:04:15.250 PM getHour will return 22.
	 * @param timestamp
	 * @return
	 */
	private static int getHour(long timestamp) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date(timestamp * 1000));
		return cal.get(Calendar.HOUR_OF_DAY);
	}
	
	
	public void shutdown(){
		client.flush();
		client.shutdown();
	}
}
