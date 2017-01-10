package edu.tamu.isys.attacks;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//input1 ("src<>Broadcast<>ARP", IP_from_info)
//input2 ("src<>prot<>Echo (ping)", dst)
//calculate ("src<>Broadcast<>ARP", count_of_IPs)
//calculate ("src<>prot<>Echo (ping)", count_of_dsts)
//output ("src scanned network using protocol: ", prot)

public class HostScanReducer  extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		Map<String, Integer> map  = new HashMap<String, Integer>();
		
		String keyString = key.toString();
		String[] keySplit = keyString.split("<>");
		String source = keySplit[0];
		String protocol = keySplit[2];
		
		String newKeyString = "";
		
		int tempVal=0;
		int hostsScanned = 0;
		for (Text value : values){  
			String valString = value.toString();
			if (map.containsKey(valString)){
				tempVal = map.get(valString)+1; //don't necessarily need a count but I take it anyway
				map.put(valString, tempVal);
			}
			else{
				map.put(valString, 1);
			}
		}
		hostsScanned = map.size();
		newKeyString = (source+" scanned the network with protocol: ");
		if (hostsScanned > 250){
			context.write(new Text(newKeyString), new Text(protocol));
		}
	}

}
