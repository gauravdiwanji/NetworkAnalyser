package edu.tamu.isys.attacks;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//input (src||dst||TCP, %SS.SSSSS<>info)
public class DDOSReducer extends Reducer<Text, Text, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		int integral_time,temp_value;
		LinkedHashMap<Integer,Integer> time_count_map = new LinkedHashMap<Integer,Integer>();
		String mainKey = key.toString();
		String keySplit[] = mainKey.split("\\|\\|");
		String attacker = keySplit[0];
		String victim = keySplit[1];
				
		for (Text val : values) {
						
			String[] data_column = val.toString().split("<>");
			double time_double = Double.parseDouble(data_column[0]);
			integral_time = -1;
			integral_time =  (int) Math.floor(time_double);
			
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
		}//ROF
		
		Set<Integer> map_iterator = time_count_map.keySet(); //01, 02, 03, 04, ...n for each src||dst||TCP
		
		int tmpVal=0, secCt=0;
		for (Integer current_key : map_iterator) //01, 02, 03, 04, ...n for each src||dst||TCP
		{
			if (time_count_map.get(current_key) > 1000)
			{
				secCt+=1;									
				tmpVal += time_count_map.get(current_key);  
			}
		}
		
		Text key_text = new Text(attacker+" attacked "+victim+" for "+secCt+" seconds with a pckt ct of: ");
		IntWritable value_iw = new IntWritable(tmpVal);
		
		if (tmpVal > 1000)
		{
		context.write(key_text,value_iw);
		}
		
	}
}