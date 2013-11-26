package com.felix.hadoop.training.sortedActor;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ActorCountMap extends Mapper<LongWritable, Text, ActorCompositeKey, Text>{

	private ActorCompositeKey ackey = new ActorCompositeKey();
	
	@Override
	public void setup(Context context){
		System.out.println(">>>>>>Mapper>>>>>>SETUP Called>>>"+context.getJobName());
	}
	@Override
	public void map(LongWritable inputKey, Text inputVal, Context context) throws IOException, InterruptedException{
		String row = inputVal.toString();
		String[] rowAttribute = row.trim().split(",");	
		/*
		 * Input rowAttribute = [Ajay,2, 1980]
		 * Assuming that data would be well formed
		 */
		ackey.clear();
		ackey.setActorName(rowAttribute[0]);
		ackey.setMovieCount(rowAttribute[1]);
		ackey.setYearName(rowAttribute[2]);
		System.out.println(ackey.getActorName()+ ">>>>>>>"+ ackey.getMovieCount());
		context.write(ackey, new Text(rowAttribute[1]));
	}
	@Override
	public void cleanup(Context context){
		System.out.println(">>>>>>Mapper>>>>>>CLEANUP Called>>>"+context.getJobName());
	}
}
