package com.felix.hadoop.training.sortedActor;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * The final thing left is the secondary sorting over movieCount field. 
 * To achieve this we shall just create our own comparator which first checks for 
 * the equality of actorName and only if they are equal goes on to check for the movieCount field. 
 * This shall be implemented as follows and is pretty much self-explanatory:
 */
public class ActorCompositeKeyComparator extends WritableComparator {

	protected ActorCompositeKeyComparator() {
		super(ActorCompositeKey.class,true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2){
		ActorCompositeKey ack1 = (ActorCompositeKey)w1;
		ActorCompositeKey ack2 = (ActorCompositeKey)w2;
		
		//belong to the same actor?
		int result= ack1.getActorName().compareTo(ack2.getActorName());
		if(result ==0){
			//System.out.println(ack1.getActorName()+":::::"+ ack1.getMovieCount());
			//Only if they belong to the same actor, we should sort them by value
			return ack1.getMovieCount().compareTo(ack2.getMovieCount());
		}
		if(result ==0){
			//System.out.println(ack1.getActorName()+":::::"+ ack1.getMovieCount());
			//Only if they belong to the same actor, we should sort them by value
			return ack1.getYearName().compareTo(ack2.getYearName());
		}
		return result;
		
	}

}
