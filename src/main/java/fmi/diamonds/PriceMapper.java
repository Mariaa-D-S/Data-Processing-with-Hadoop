package fmi.diamonds;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PriceMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, DoubleWritable> {
	
	String cutFilter;
	String colorFilter;
	String clarityFilter;
	
	@Override
	public void configure(JobConf job) {
		cutFilter = job.get("filter1");
		colorFilter = job.get("filter2");
		clarityFilter = job.get("filter3");
	}
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		
		int keyInt = (int) key.get();
		
		if(keyInt == 0) {
			return;
		}else {

		String[]columns = value.toString().split(",");
		
		if (columns.length < 7) {
	        System.out.println("Invalid row " + value.toString());
	        return;
	    }
        
        String cut = columns[1].replace("\"", "").toLowerCase();
        String color = columns[2].replace("\"", "").toLowerCase();
        String clarity = columns[3].replace("\"", "").toLowerCase();
        
        double price = 0;
        try {
            price = Double.parseDouble(columns[6].replace("\"", ""));
        } catch (NumberFormatException e) {
        	System.out.println("Invalid price format in row: " + value.toString());
            return;
        }
        							//change to equals()
        if ((cutFilter == null || cut.contains(cutFilter.toLowerCase())) && 
            (colorFilter == null || color.contains(colorFilter.toLowerCase())) && 
            (clarityFilter == null || clarity.contains(clarityFilter.toLowerCase()))) {
            
            Text outputKey = new Text(columns[1] + "-" + columns[2] + "-" + columns[3]);
            try {
            output.collect(outputKey, new DoubleWritable(price));
            System.out.println("Mapper: " + outputKey.toString() + " with value: " + price);
            }catch(IOException e) {
                System.out.println("Error collecting in Mapper: " + e.getMessage());
                e.printStackTrace();
        }
    }
		
		}
		}

}
