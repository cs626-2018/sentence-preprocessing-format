package cs626.sentence.preprocessing.format;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class FilePartitioner implements Partitioner<Text, Text> {
	
	@Override
	public void configure(JobConf job) {	
	}
	
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		// Hash the key (filename) and divide by the number of partitions (number of files)
		return Math.abs((key.toString().hashCode()) % numPartitions);
	}
}
