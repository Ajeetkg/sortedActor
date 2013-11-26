package com.felix.hadoop.training.sortedActor;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ActorCountReduce extends Reducer<ActorCompositeKey, Text, Text, Text>{

	@Override
	public void setup(Context context){
		System.out.println(">>>REDUCER>>>SETUP"+context.getJobName());
	}
	
	public void reduce(ActorCompositeKey ackey,Iterable<Text> listofvalues, Context context) throws IOException, InterruptedException{
		int totalMovies=0;
		
		
		for(Text countOfMovies:listofvalues){
			//System.out.println(ackey.getActorName()+"---------"+countOfMovies+ "-----" +totalMovies);
			//totalMovies=totalMovies+Integer.parseInt(countOfMovies.toString());
			System.out.println(ackey.toString());
			context.write(new Text(ackey.getActorName()),new Text(countOfMovies+"::"+ackey.getYearName()));
		}
		
		
		
		
	}
	
	@Override
	public void cleanup(Context context){
		System.out.println(">>>REDUCER>>>CLEANUP"+context.getJobName());
	}
}
