package edu.tamu.isys.attacks;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class DDOSReducer extends Reducer<Text, Text, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		int integral_time,temp_value;
		
		LinkedHashMap<Integer,Integer> time_count_map = new LinkedHashMap<Integer,Integer>();
				
		for (Text val : values) {
			
						
			String[] data_column = val.toString().split("<>");
			double time_double = Double.parseDouble(data_column[0]);
			integral_time = -1;
			integral_time =  (int) Math.floor(time_double);
			//System.out.println("INFO Value: "+data_column[2]);
			//context.write(new Text(key.toString()+"==>"+data_column[0]+"==>"+data_column[1]), new DoubleWritable(0.0));
			
				if (data_column[1].contains("ACK"))
				{ 
				//System.out.println("ACK: "+val.toString());
				//context.write(new Text("ACK:"+key), new DoubleWritable(0.0));
				}
				else if(time_count_map.containsKey(integral_time))
				{	
					temp_value = time_count_map.get(integral_time) +1;
					time_count_map.put(integral_time,temp_value);
				}
				else
				{
					temp_value = 1;
					time_count_map.put(integral_time,temp_value);
				}
			
		
		}
		
		Set<Integer> map_iterator = time_count_map.keySet();
		
		int max_key=0 ,max_value = 0;
		for (Integer current_key : map_iterator)
		{
			if (time_count_map.get(current_key) > max_value )
			{max_value = time_count_map.get(current_key);
			max_key = current_key;}
		}
		
		Text key_text = new Text (key.toString()+"||"+max_key);
		
		DoubleWritable value_dw = new DoubleWritable(max_value);
		
		if (max_value > 1000)
		{
		context.write(key_text,value_dw);
		}
		
		
		
	}
}