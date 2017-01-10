package edu.tamu.isys.attacks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Program2 {
		
	public static void main(String[] args) throws Exception{
	
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DDoS");
		job.setJarByClass(Program2.class);
		
		job.setMapperClass(DDOSMapper.class);
		job.setReducerClass(DDOSReducer.class);
		
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true)?0:1);

	}
}
