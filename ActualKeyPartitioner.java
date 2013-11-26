package com.felix.hadoop.training.sortedActor;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;

/*
 * Secondly, we need to implement our own partitioner. 
 * The reason we need to do so is that now we are emitting both actorName and movieCount 
 * as the key and the defaut partitioner (HashPartitioner) would then not be able 
 * to ensure that all the records related to a certain actorName comes to the same 
 * reducer (partition). Hence, we need to make the partitioner to only consider 
 * the actual key part (actorName) while deciding on the partition for the record. 
 * This can be done as follows:
 */
public class ActualKeyPartitioner extends Partitioner<ActorCompositeKey, Text>{

	HashPartitioner< Text, Text> hashpartitioner = new HashPartitioner<Text,Text>();
	Text newKey = new Text();
	
	@Override
	public int getPartition(ActorCompositeKey ackey, Text value, int numReduceTasks) {
		// TODO Auto-generated method stub
		
		try {
			newKey.set(ackey.getActorName());
			//System.out.println(newKey+"-----------"+value);
			return hashpartitioner.getPartition(newKey, value, numReduceTasks);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		}
	}

}
