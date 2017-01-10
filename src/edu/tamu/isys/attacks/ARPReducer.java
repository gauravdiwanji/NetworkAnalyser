package edu.tamu.isys.attacks;

import java.io.IOException;
import java.util.LinkedList;
import java.util.ListIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ARPReducer  extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		LinkedList<String> ip_list = new LinkedList<String>();
		
		for (Text val : values) {
		if	(!ip_list.contains(val.toString()))
		{
			ip_list.add(val.toString());
		}	
		}
		
		if (ip_list.size() > 1)
		{
		    ListIterator<String> listIterator = ip_list.listIterator();
		    String list_ips = new String();
		    
	        while (listIterator.hasNext()) {
	        	list_ips = list_ips + listIterator.next() + " ";
	        }
	        
	        context.write(key,new Text(list_ips));
	 
		}
	}
}