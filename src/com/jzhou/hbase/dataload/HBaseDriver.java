package com.jzhou.hbase.dataload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HBaseDriver {

	//private static Log LOG = LoggerFactory.getLogger(HBaseDriver.class);
	private static final Log LOG = LogFactory.getLog(HBaseDriver.class);

	/**
	 * Main entry point for the example.
	 * 
	 * @param args
	 *            arguments
	 * @throws Exception
	 *             when something goes wrong
	 */
	public static void main(String[] args) throws Exception {
		LOG.info("Code started");
              //HBaseDriver.connectHBase(); // Initializing connection with HBase
		Configuration config = HBaseConfiguration.create();
		
		String inputPath=args[0];
		String outputPath=args[1];
		String tableName=args[2];

	//	config.set("hbase.table.name", tableName);
		config.set("xmlinput.start", "<domain.web.V1WebCru>");
		config.set("xmlinput.end", "</domain.web.V1WebCru>");

		Job job = new Job(config, "HBase_Bulk_loader");
		job.setJarByClass(HBaseDriver.class);
		
		job.setInputFormatClass(XmlInputFormat.class);

		job.setMapperClass(HBaseMapper.class);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);

		job.setNumReduceTasks(0);

		HTable htable = new HTable(config, tableName);
		HFileOutputFormat2.configureIncrementalLoad(job, htable);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);

		// Importing the generated HFiles into a HBase table
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(config);
		loader.doBulkLoad(new Path(outputPath), htable);
		LOG.info("Code ended");
	}

}
