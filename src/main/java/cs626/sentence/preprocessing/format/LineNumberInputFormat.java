package cs626.sentence.preprocessing.format;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import com.google.common.base.Charsets;

import cs626.sentence.preprocessing.format.LineNumberReader;

// NOTE: Hadoop HDFS default block size is 64MB
// 		 Cloudera Hadoop default block size is 128MB

public class LineNumberInputFormat extends TextInputFormat {
	private static final Log LOG = LogFactory.getLog(LineNumberInputFormat.class.getName());
	
	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
		reporter.setStatus(genericSplit.toString());
	    String delimiter = job.get("textinputformat.record.delimiter");
	    byte[] recordDelimiterBytes = null;
	    if (null != delimiter) {
	      recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
	    }
	    return new LineNumberReader(job, (FileSplit) genericSplit, recordDelimiterBytes);	
	}
	// Override isSplitable to always return false, it will not split files into separate chunks
	@Override
	protected boolean isSplitable(FileSystem fs, Path file) {
		return false;
	}
}

