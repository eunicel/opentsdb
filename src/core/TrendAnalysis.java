package net.opentsdb.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.nio.ByteBuffer;

import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import org.slf4j.Logger;

public class TrendAnalysis {
	
	private static HBaseClient client;
	private final Config config;
	private TSDB tsdb;
	
	private static byte[] trends_table = "trends".getBytes();
	private static final byte[] TRENDS_FAMILY = {'r'};
	private static final byte[] T_FAMILY = {'t'};

	private static byte[] tsdb_table = "tsdb".getBytes();
	private static final byte[] T_QUALIFIER = {'t'};
	
	private Map<String, Long> queue;
	
	static Logger log = LoggerFactory.getLogger(TrendAnalysis.class);

	/**
	 * Creates a trends table in HBase that stores the mean
	 * and standard deviation for each hour of each day of
	 * @param client
	 * @param config
	 */
	public TrendAnalysis(HBaseClient client, final Config config, TSDB tsdb){
		log.info("in TrendAnalysis constructor");
		this.client = client;
		this.config = config;
		this.tsdb = tsdb;
		
		queue = Collections.synchronizedMap(new LinkedHashMap<String, Long>());

		startThread();
	}
	
	/**
	 * Alternate constructor
	 * @param config An initialized configuration object
	 */
	public TrendAnalysis(final Config config, TSDB tsdb) {
	    this(new HBaseClient(config.getString("tsd.storage.hbase.zk_quorum"),
	                         config.getString("tsd.storage.hbase.zk_basedir")),
	    		config, tsdb);
	  }
	
	private void startThread() {
		Thread thread = new Thread(new Runnable() {
			public void run() {
				while(true) {
					try{
						log.info("start sleep");
						Thread.sleep(20000); // Sleeps for 2 hours -- 20 sec for testing
						log.info("done sleeping");
					} catch (Exception e) {
						e.printStackTrace();
					}
					log.info("start looking through the queue");
					Iterator<String> iterator = queue.keySet().iterator();
					while(iterator.hasNext()) {
						String point = iterator.next();
						log.info("looking at row : " + point);
						long timeAdded = queue.get(point);
						long currentTime = System.currentTimeMillis() / 1000L;

						// ensures no more data points will be added to this hour
						if(currentTime > timeAdded + 20L) { // added > 2 hours ago -- 20 sec for testing
							log.info("row added > 10 sec ago");
							updateTrendData(point);
							iterator.remove();
						} else {
							// points are sorted by time added so
							// all points after this will be even more recent
							break;
						}
					}
				}
			}
		});
		thread.start();
	}

	private void updateTrendData(String dataPoint) {
		try {
			log.info("updating trend data");
			// get info from dataPoint
			String metric = getMetricFromPoint(dataPoint);
			Map<String, String> tags = getTagsFromPoint(dataPoint);
			long timestamp = getTimestampFromPoint(dataPoint);
			
			String row = getTrendsRowKey(metric, tags);
			ArrayList<KeyValue> results = getRowResults(row);
			log.info("row = " + row);
			log.info("results.size() = " + results.size());
			if(results.size() == 0) {
				initializeNewRows(row, dataPoint);
				results = getRowResults(row);
				log.info("after initialization. row = " + row);
				log.info("after intiialization. results.size() = " + results.size());
			}

			byte[] timestampBytes = results.get(0).value();	// get stored timestamp from trends table
			long storedTimestamp = ByteBuffer.wrap(timestampBytes).getLong();
			log.info("stored timestamp = " + storedTimestamp);
			long pointTimestamp = getTimestampFromPoint(dataPoint); // get timestamp from data point
			log.info("point timestamp = " + pointTimestamp);
			
			if (pointTimestamp > storedTimestamp) {
				log.info("comparing stored with point's timestamp");
			    final byte[] rowKey = IncomingDataPoints.rowKeyTemplate(tsdb, metric, tags);
			    final long base_time;
			    //final byte[] qualifier = Internal.buildQualifier(timestamp, flags);
			    if ((timestamp & Const.SECOND_MASK) != 0) {
			        // drop the ms timestamp to seconds to calculate the base timestamp
			        base_time = ((timestamp / 1000) - 
			            ((timestamp / 1000) % Const.MAX_TIMESPAN));
			      } else {
			        base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
			      }
			    Bytes.setInt(rowKey, (int) base_time, tsdb.metrics.width());
				GetRequest getData = new GetRequest(tsdb_table, rowKey, T_FAMILY);
				
				ArrayList<KeyValue> dataResults = client.get(getData).join();
				log.info("got results = " + dataResults);
				for(KeyValue kv : dataResults) {
					log.info("kv timestamp = " + kv.timestamp() + ", " + kv.value().toString());			
				}
			} else {
				log.info("already updated");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private ArrayList<KeyValue> getRowResults(String row) {
		ArrayList<KeyValue> results = new ArrayList<KeyValue>();
		try {
			String rowCount = row + "-count"; // get count (if count exists, mean and stdev should too)
			final byte[] rowCountBytes = rowCount.getBytes();
			GetRequest getTimestamp = new GetRequest(trends_table, rowCountBytes, T_FAMILY, T_QUALIFIER);
			results = client.get(getTimestamp).join();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;
	}
	
	/**
	 * initialize 3 rows (count, mean, stdev) in trends table
	 * for trends family and timestamp for each row.
	 * @param row
	 * @param point
	 */
	private void initializeNewRows(String row, String point) {
		log.info("initializing new rows");

		// build qualifier
		long timestamp = getTimestampFromPoint(point);
		int day = getDay(timestamp);
		int hour = getHour(timestamp);
		String qualifier = day + "-" + hour;
		
		// create put requests for new rows
		byte[] countBytes = new byte[8];
		String countRow = row + "-count";
		ByteBuffer.wrap(countBytes).putDouble(1); // initialize count to 1
		KeyValue countKv =
				new KeyValue(countRow.getBytes(), TRENDS_FAMILY, qualifier.getBytes(), countBytes);
		
		long value = getValueFromPoint(point);
		byte[] meanBytes = new byte[8];
		String meanRow = row + "-mean";
		ByteBuffer.wrap(meanBytes).putDouble(value); // initialize mean to value of data point
		KeyValue meanKv =
				new KeyValue(meanRow.getBytes(), TRENDS_FAMILY, qualifier.getBytes(), meanBytes);
		
		byte[] stdevBytes = new byte[8];
		String stdevRow = row + "-standard_deviation";
		ByteBuffer.wrap(stdevBytes).putDouble(0); // initialize standard deviation to 0
		KeyValue stdevKv =
				new KeyValue(stdevRow.getBytes(), TRENDS_FAMILY, qualifier.getBytes(), stdevBytes);
		
		byte[] timeBytes = new byte[8];
		ByteBuffer.wrap(timeBytes).putLong(0L); // initialize last timestamp to 0
		KeyValue timeKvCount = 
				new KeyValue(countRow.getBytes(), T_FAMILY, T_QUALIFIER, timeBytes);
		KeyValue timeKvMean = 
				new KeyValue(meanRow.getBytes(), T_FAMILY, T_QUALIFIER, timeBytes);
		KeyValue timeKvStdev = 
				new KeyValue(stdevRow.getBytes(), T_FAMILY, T_QUALIFIER, timeBytes);
	
		PutRequest count = new PutRequest(trends_table, countKv);
		PutRequest mean = new PutRequest(trends_table, meanKv);
		PutRequest stdev = new PutRequest(trends_table, stdevKv);
		PutRequest timeCount = new PutRequest(trends_table, timeKvCount);
		PutRequest timeMean = new PutRequest(trends_table, timeKvMean);
		PutRequest timeStdev = new PutRequest(trends_table, timeKvStdev);

		client.put(count);
		client.put(mean);
		client.put(stdev);
		client.put(timeCount);
		client.put(timeMean);
		client.put(timeStdev);
		client.flush();
		log.info("NEW ROWS ADDED");
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
	public void addPoint(String metric, byte[] value,
			long timestamp, Map<String, String> tags, short flags) {
		log.info("trendAnalysis adding point!!!!!!!!!!!!!! !!!!!!!!");
		
		// add new data point to queue
		String dataPoint = getDataString(metric, value, timestamp, tags, flags);
		long currentTime = System.currentTimeMillis() / 1000L;

	    if(queue.containsKey(dataPoint)) {
	    	log.info("removing " + dataPoint + " from queue");
	    	queue.remove(dataPoint); // remove and insert to update order
	    }
	    log.info("adding " + dataPoint + " to queue");
    	queue.put(dataPoint, currentTime);
	}
	
	private String getTrendsRowKey(String metric, Map<String, String> tags) {
		ArrayList<String> tagsList = new ArrayList<String>(tags.keySet());
		Collections.sort(tagsList);
		log.info("size of tagsList is======" + tagsList.size());
		String tagsAndValues = "";
		for(String tag : tagsList) {
			tagsAndValues = tagsAndValues + "-" + tag + "=" + tags.get(tag);
		}
		log.info("trends row key = " + metric + tagsAndValues);
		return metric + tagsAndValues;
	}
	private String getDataString(String metric, byte[] value,
			long timestamp, Map<String, String> tags, short flags) {
		log.info("getDataString!!!!!!!!!!!!!!!!!!");
		long v = 0;
		for (int i = 0; i < value.length; i++)
		{
		   v += ((long) value[i] & 0xffL) << (8 * i);
		}
		return getTrendsRowKey(metric, tags) + "-" + String.valueOf(flags)
			+ "-" + String.valueOf(timestamp) + "-" + v;
	}
	
	/**
	 * Given the rowKey, return the metric name.
	 * @param rowKey
	 * @return metric
	 */
	private String getMetricFromPoint(String point) {
		return point.split("-")[0];
	}
	
	private short getFlagsFromPoint(String point) {
		String[] rowData = point.split("-");
		String flag = rowData[rowData.length-3];
		return Short.parseShort(flag);
	}
	
	
	private long getTimestampFromPoint(String point) {
		String[] rowData = point.split("-");
		String timestamp = rowData[rowData.length-2];
		return Long.parseLong(timestamp);
	}
	
	private long getValueFromPoint(String point) {
		String[] rowData = point.split("-");
		String value = rowData[rowData.length-1];
		return Long.parseLong(value);
	}
	
	/**
	 * Given the rowKey, returns a mapping of the tags to their values.
	 * @param rowKey
	 * @return tags
	 */
	private Map<String, String> getTagsFromPoint(String point) {
		log.info("getting tags from point");
		Map<String, String> tags = new HashMap<String, String>();
		String[] rowData = point.split("-");
		// exclude first and last two data (metric, flags, base_time)
		for(int i = 1; i < rowData.length-3; i++) {
			String[] tagPair = rowData[i].split("=");
			tags.put(tagPair[0], tagPair[1]);
		}
		return tags;
	}
	
	/**
	 * Updates the stats for a particular row with the new value.
	 * Updates standard deviation according
	 * https://en.wikipedia.org/wiki/Standard_deviation#Population-based_statistics
	 * @param stats An array of count, mean, and standard deviation
	 * @param value The new value to add
	 * @return stats The updated coutn, mean, and standard deviation
	 */
	private double[] updateStats(double[] stats, long value) {
		double count = stats[0];
		double mean = stats[1];
		double stdev = stats[2];
		
		stats[1] = ((mean * count) + value) / (count + 1); // update mean
		stats[0] += 1; // update count
		stats[2] = Math.sqrt((count * Math.pow(stdev, 2) / (count + 1)
				+ (count / Math.pow((count + 1), 2))
				* Math.pow((mean - value), 2))); // update standard deviation
		log.info("updated count = " + stats[0]);
		log.info("updated mean = " + stats[1]);
		log.info("updated stdev = " + stats[2]);
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
