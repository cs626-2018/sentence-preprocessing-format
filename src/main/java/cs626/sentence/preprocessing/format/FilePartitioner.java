package cs626.sentence.preprocessing.format;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FilePartitioner extends Partitioner<Text, Text> implements Configurable {

	private Configuration conf = null;

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}
	
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		// Hash the key (filename) and divide by the number of partitions (number of files)
		return Math.abs((key.toString().hashCode()) % numPartitions);
	}
}
