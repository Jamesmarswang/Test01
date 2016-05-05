package Partationer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import Mypackage.UDFkey;

public class KeyPartitioner extends Partitioner<UDFkey, Text> {

	@Override
	public int getPartition(UDFkey key, Text value, int numPartitioner) {
		return (key.getSecondKey().hashCode() % numPartitioner);
	}

}
