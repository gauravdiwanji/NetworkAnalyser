package edu.tamu.isys.attacks;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DDOSMapper extends Mapper<LongWritable, Text, Text, Text>
{
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			
		String data_row = value.toString();

		System.out.println("Now Processing: "+data_row);
		
		Pcap pcap = new Pcap();
		int write_pcap = pcap.writeFromCSV(data_row);
		
		if (write_pcap == 0)
		{return;}
		

		Text src_key = new Text();
		src_key.set(pcap.getSource()+"||"+pcap.getDestination()+"||"+pcap.getProtocol());
		
		Text time_info_value = new Text(pcap.getTime()+"<>"+pcap.getInfo());
		//DoubleWritable time_value = new DoubleWritable(pcap.getTime());
			
		context.write(src_key, time_info_value);
						
	}	

}
