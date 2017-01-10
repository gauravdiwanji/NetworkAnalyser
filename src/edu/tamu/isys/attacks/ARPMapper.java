package edu.tamu.isys.attacks;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ARPMapper extends Mapper<LongWritable, Text, Text, Text>
{
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			
		String data_row = value.toString();

		//System.out.println("Now Processing: "+data_row);
		
		Pcap pcap = new Pcap();
		int write_pcap = pcap.writeFromCSV(data_row);
		
		if (write_pcap == 0)
		{return;}
		else if (!pcap.getProtocol().equals("ARP"))
		{return;}
		

		Text src_key = new Text();
		src_key.set(pcap.getSource());
		
		String ip_s = new String();
		String info_s = pcap.getInfo();
		
		if (info_s.contains("Who has"))
		{
			ip_s = info_s.substring(info_s.indexOf("Tell ")+5,info_s.length());
		}
		else if (info_s.contains("is at"))
		{
			ip_s = info_s.substring(0,info_s.indexOf(" is at"));
		}
		else
		{			
		}
		
		Text ip_value = new Text(ip_s);
		//DoubleWritable time_value = new DoubleWritable(pcap.getTime());
			
		context.write(src_key, ip_value);
						
	}	

}