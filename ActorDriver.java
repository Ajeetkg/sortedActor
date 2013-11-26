package com.felix.hadoop.training.sortedActor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class ActorDriver extends Configured implements Tool{

	
	public static void main(String[] args) throws Exception{
		ToolRunner.run(new ActorDriver(),args);
	}
	@Override
	public int run(String[] arg0) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker","local");
		
		Job job = new Job(conf);
		
		job.setMapperClass(ActorCountMap.class);
		job.setReducerClass(ActorCountReduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		//job.setMapOutputKeyClass(ActorCompositeKey.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(2);
		
		job.setMapOutputKeyClass(ActorCompositeKey.class);
		job.setPartitionerClass(ActualKeyPartitioner.class);
		job.setGroupingComparatorClass(ActualKeyGroupingPartitioner.class);
		job.setSortComparatorClass(ActorCompositeKeyComparator.class);
		
		FileInputFormat.addInputPath(job, new Path("/home/training/training_material/data/Interview"));
		//FileOutputFormat.setOutputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path("/home/training/OutputData/ActorsNewData2"));

		job.waitForCompletion(true);
		
		
		return 0;
	}
	
	

	
}
