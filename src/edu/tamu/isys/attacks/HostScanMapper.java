package edu.tamu.isys.attacks;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//output (src<>Broadcast<>ARP, IP)
public class HostScanMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String data = value.toString();
		Pcap pcap = new Pcap();

		int write_pcap = pcap.writeFromCSV(data);
		if (write_pcap == 0) {return;}
		
		String src = pcap.getSource();
		String dst = pcap.getDestination();
		String prot = pcap.getProtocol();
		String information = pcap.getInfo();
		String arpKeyString = (src+"<>"+dst+"<>"+prot);
		String icmpType = "";
		String icmpKeyString = "";
		String arpDest = "";
		
		if (prot.equals("ARP")){
			if (information.contains("Who has")){
				arpDest = information.substring("Who has ".length(), information.indexOf("?"));
				System.out.print(arpKeyString+" "+arpDest+"\n");
				context.write(new Text(arpKeyString), new Text(arpDest));
			}
			else{}
		}
		else if (prot.equals("ICMP")){
			if (information.contains("Echo")){
				icmpType = information.substring(0, information.indexOf("id="));
				icmpKeyString = (src+"<>"+icmpType+"<>"+prot);
				if (icmpType.contains("request")){
					System.out.print("I CAN HAS ECHO?");
					context.write(new Text(icmpKeyString), new Text(dst));
				}
			}
		}
		else{}
		
	}
	
}
