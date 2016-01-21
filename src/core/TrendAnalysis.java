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
			short flags = getFlagsFromPoint(dataPoint);

			String row = getTrendsRowKey(metric, tags);
			ArrayList<KeyValue> results = getRowResults(row);
			if(results.size() == 0) {
				initializeNewRows(row, dataPoint);
				results = getRowResults(row);
			}

			byte[] timestampBytes = results.get(0).value();	// get stored timestamp from trends table
			long storedTimestamp = ByteBuffer.wrap(timestampBytes).getLong();
			long pointTimestamp = getTimestampFromPoint(dataPoint); // get timestamp from data point
			
			if (pointTimestamp > storedTimestamp) {
				log.info("comparing stored with point's timestamp");
			    byte[] TSDBRowKey = getTSDBRowKey(metric, tags, pointTimestamp);
				GetRequest getData = new GetRequest(tsdb_table, TSDBRowKey, T_FAMILY);
				
				// look through row backwards to find new data points 
				ArrayList<KeyValue> dataResults = client.get(getData).join();
				log.info("got results = " + dataResults);
				ArrayList<Double> newPoints = new ArrayList<Double>();
				for(int i = dataResults.size()-1; i >= 0; i--) {
					KeyValue kv = dataResults.get(i);
					long dataPointBaseTime = Bytes.getUnsignedInt(kv.key(),
							tsdb.metrics.width()); // gets base time from row key
					long dataPointTimestamp = Internal.getTimestampFromQualifier(kv.qualifier(), dataPointBaseTime);
					dataPointTimestamp = dataPointTimestamp / 1000L; // convert ms to seconds
					log.info("TIMESTAMP = " + dataPointTimestamp);
					if (dataPointTimestamp > storedTimestamp) {
						long value = 0;
						for (int j = 0; j < kv.value().length; j++)
						{
						   value += ((long) kv.value()[j] & 0xffL) << (8 * j);
						}
						newPoints.add((double)value);
					} else {
						break;
					}
				}
				String trendsQualifier = getTrendsQualifier(pointTimestamp);
				String trendsRowKey = getTrendsRowKey(metric, tags);
				updateTrendsRow(trendsRowKey, trendsQualifier, newPoints);
			} else {
				log.info("already updated");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Calculate and update trends for row.
	 * https://en.wikipedia.org/wiki/Standard_deviation#Population-based_statistics
	 * @param rowKey
	 * @param qualifier
	 * @param newPoints
	 */
	private void updateTrendsRow(String rowKey, String qualifier, ArrayList<Double> newPoints) {
		try {
			// get old trends
			String countRowKey = rowKey + "-count";
			GetRequest countRequest = new GetRequest(trends_table, countRowKey.getBytes(), TRENDS_FAMILY, qualifier.getBytes());
			byte[] countBytes = client.get(countRequest).join().get(0).value();
			double oldCount = ByteBuffer.wrap(countBytes).getDouble();
			
			String meanRowKey = rowKey + "-mean";
			GetRequest meanRequest = new GetRequest(trends_table, meanRowKey.getBytes(), TRENDS_FAMILY, qualifier.getBytes());
			byte[] meanBytes = client.get(countRequest).join().get(0).value();
			double oldMean = ByteBuffer.wrap(meanBytes).getDouble();

			String stdevRowKey = rowKey + "-standard_deviation";
			GetRequest stdevRequest = new GetRequest(trends_table, stdevRowKey.getBytes(), TRENDS_FAMILY, qualifier.getBytes());
			byte[] stdevBytes = client.get(stdevRequest).join().get(0).value();
			double oldStdev = ByteBuffer.wrap(stdevBytes).getDouble();
			
			// get trends of new points
			double newCount = newPoints.size();
			double sum = 0;
			for(double point : newPoints) {
				sum += point;
			}
			double newMean = sum / newCount;
			double diffSqSum = 0;
			for(double point : newPoints) {
				diffSqSum += diffSqSum = Math.pow(point - newMean, 2);
			}
			double newStdev = diffSqSum / (newCount - 1);
			
			// update old trends based on new points
			double updatedCount = oldCount + newCount;
			double updatedMean = (oldCount * oldMean + newCount * newMean) / (oldCount + newCount);
			double updatedStdev = Math.sqrt(
					(oldCount * Math.pow(oldStdev, 2) + newCount * Math.pow(newStdev, 2)) / (oldCount + newCount)
					+ ((oldCount * newCount) / (oldCount + newCount)) * Math.pow(oldStdev - newStdev, 2));
			
			// create KeyValue and PutRequests for updated trends
			byte[] newCountBytes = new byte[8];
			ByteBuffer.wrap(newCountBytes).putDouble(updatedCount);
			KeyValue countKv =
					new KeyValue(countRowKey.getBytes(), TRENDS_FAMILY, qualifier.getBytes(), newCountBytes);
			byte[] newMeanBytes = new byte[8];
			ByteBuffer.wrap(newMeanBytes).putDouble(updatedMean); 
			KeyValue meanKv =
					new KeyValue(meanRowKey.getBytes(), TRENDS_FAMILY, qualifier.getBytes(), newMeanBytes);
			byte[] newStdevBytes = new byte[8];
			ByteBuffer.wrap(newStdevBytes).putDouble(updatedStdev); 
			KeyValue stdevKv =
					new KeyValue(stdevRowKey.getBytes(), TRENDS_FAMILY, qualifier.getBytes(), newStdevBytes);
			
			PutRequest c = new PutRequest(trends_table, countKv);
			PutRequest m = new PutRequest(trends_table, meanKv);
			PutRequest s = new PutRequest(trends_table, stdevKv);
			
			client.put(c);
			client.put(m);
			client.put(s);
			client.flush();
		
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
		String qualifier = getTrendsQualifier(timestamp);
		
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
	
	private byte[] getTSDBRowKey(String metric, Map<String, String> tags, long timestamp) {
		final byte[] rowKey = IncomingDataPoints.rowKeyTemplate(tsdb, metric, tags);
	    final long base_time;
	    if ((timestamp & Const.SECOND_MASK) != 0) {
	        base_time = ((timestamp / 1000) - 
	            ((timestamp / 1000) % Const.MAX_TIMESPAN));
	      } else {
	        base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
	      }
	    Bytes.setInt(rowKey, (int) base_time, tsdb.metrics.width());
	    return rowKey;
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
	 * Given the timestamp, builds the trends table qualifier in the
	 * format of Day-Hour where Sunday = 1, Monday = 2, etc.
	 * and 10:04:15.250 PM = 22.
	 * @param timestamp
	 * @return trends table qualifier
	 */
	private String getTrendsQualifier(long timestamp) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date(timestamp * 1000));
		return cal.get(Calendar.DAY_OF_WEEK) + "-" + cal.get(Calendar.HOUR_OF_DAY);
	}
	
	public void shutdown(){
		client.flush();
		client.shutdown();
	}
}
