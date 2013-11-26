package com.felix.hadoop.training.sortedActor;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * Now, the partitioner only makes sure that the all records related to the same actorname 
 * comes to a particular reducer, but it doesn't guarantee that all of them will come in 
 * the same input group (i.e. in a single reduce() call as the list of values). 
 * In order to make sure of this, we will need to implement our own grouping comparator. 
 * We shall do similar thing as we did for the partitioner, i.e. only look at the actual key 
 * (actorName) for grouping of reducer inputs. This can be done as follows:
 */
public class ActualKeyGroupingPartitioner extends WritableComparator{

	protected ActualKeyGroupingPartitioner() {
		super(ActorCompositeKey.class,true);
		// TODO Auto-generated constructor stub
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2){
		ActorCompositeKey ack1 = (ActorCompositeKey)w1;
		ActorCompositeKey ack2 = (ActorCompositeKey)w2;
		//System.out.println(ack1.getActorName()+"--OOOO--"+ack2.getActorName());
		return ack1.getActorName().compareTo(ack2.getActorName());
	}
}
