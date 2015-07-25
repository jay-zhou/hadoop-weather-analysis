package com.jzhou.hbase.dataload;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class HBaseMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	final static byte[] COL_FAMILY = "mp".getBytes();

	ParseXml parseXml = new ParseXml();
	ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	KeyValue kv;

	private static final Log LOG = LogFactory.getLog(HBaseMapper.class);
	//private static Logger LOG = LoggerFactory.getLogger(HBaseMapper.class);
	/**
	 * Map method gets XML data from tag <data> to </data>. To read the xml content the data is sent to getXmlTags method
	 * which parse the XML using STAX parser and returns an String array of contents.
	 * String array is iterated and each elements are stored in KeyValue
	 * 
	 */
	public void map(LongWritable key, Text value, Context context)
			throws InterruptedException, IOException {
		String line = value.toString();

		HashMap<String, String> fields = parseXml.getXmlTags(line);
            
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String shortName = fileName.substring(fileName.length()-7, fileName.length()-4);
        
		hKey.set(shortName.getBytes());

		Set set = fields.entrySet();
	     
	    Iterator i = set.iterator();
	    
	    while(i.hasNext()) {
	        Map.Entry me = (Map.Entry)i.next();
	         
		    if (me.getKey().equals("0")) {
			    kv = new KeyValue(hKey.get(), COL_FAMILY,
					    HColumnEnum.COL_JAN.getColumnName(),
					    me.getValue().toString().getBytes());
			    context.write(hKey, kv);
		    }

		    if (me.getKey().equals("1")) {
			    kv = new KeyValue(hKey.get(), COL_FAMILY,
					    HColumnEnum.COL_FEB.getColumnName(),
					    me.getValue().toString().getBytes());
			    context.write(hKey, kv);
		    }

		    if (me.getKey().equals("2")) {
		    	kv = new KeyValue(hKey.get(), COL_FAMILY,
					    HColumnEnum.COL_MAR.getColumnName(),
					    me.getValue().toString().getBytes());
			    context.write(hKey, kv);
		    }

	    	if (me.getKey().equals("3")) {
			    kv = new KeyValue(hKey.get(), COL_FAMILY,
					    HColumnEnum.COL_APR.getColumnName(),
					    me.getValue().toString().getBytes());
			    context.write(hKey, kv);
		    }

		    if (me.getKey().equals("4")) {
			    kv = new KeyValue(hKey.get(), COL_FAMILY,
					    HColumnEnum.COL_MAY.getColumnName(),
					    me.getValue().toString().getBytes());
			    context.write(hKey, kv);
		    }

		    if (me.getKey().equals("5")) {
			    kv = new KeyValue(hKey.get(), COL_FAMILY,
					    HColumnEnum.COL_JUN.getColumnName(),
					    me.getValue().toString().getBytes());
			    context.write(hKey, kv);
		    }

		    if (me.getKey().equals("6")) {
			    kv = new KeyValue(hKey.get(), COL_FAMILY,
					    HColumnEnum.COL_JUL.getColumnName(), 
					    me.getValue().toString().getBytes());
			    context.write(hKey, kv);
		    }

		    if (me.getKey().equals("7")) {
			    kv = new KeyValue(hKey.get(), COL_FAMILY,
					    HColumnEnum.COL_AUG.getColumnName(), 
					    me.getValue().toString().getBytes());
			    context.write(hKey, kv);
		    }
		    if (me.getKey().equals("8")) {
			    kv = new KeyValue(hKey.get(), COL_FAMILY,
					    HColumnEnum.COL_SEP.getColumnName(), 
					    me.getValue().toString().getBytes());
			    context.write(hKey, kv);
		    }
		    if (me.getKey().equals("9")) {
			    kv = new KeyValue(hKey.get(), COL_FAMILY,
					    HColumnEnum.COL_OCT.getColumnName(),
					    me.getValue().toString().getBytes());
			    context.write(hKey, kv);
		    }
		    if (me.getKey().equals("10")) {
			    kv = new KeyValue(hKey.get(), COL_FAMILY,
					    HColumnEnum.COL_NOV.getColumnName(),
					    me.getValue().toString().getBytes());
			    context.write(hKey, kv);
		    }
		    if (me.getKey().equals("11")) {
			    kv = new KeyValue(hKey.get(), COL_FAMILY,
					    HColumnEnum.COL_DEC.getColumnName(),
					    me.getValue().toString().getBytes());
			     context.write(hKey, kv);
            }
	    }
	}
}


