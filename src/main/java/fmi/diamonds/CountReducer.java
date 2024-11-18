package fmi.diamonds;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CountReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, IntWritable>{

	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException {
		int count = 0;
		while(values.hasNext()) {
			System.out.println("Processing value: " + values.next().get());
			count++;
		}
		System.out.println("Reducer output: " + key.toString() + " count: " + count);
		output.collect(key, new IntWritable(count));
	}

}
