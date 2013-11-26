package com.felix.hadoop.training.sortedActor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class ActorCompositeKey implements WritableComparable<ActorCompositeKey>{

	private String actorName;
	private String movieCount;
	private String yearName;
	
	public String getYearName() {
		return yearName;
	}

	public void setYearName(String yearName) {
		this.yearName = yearName;
	}

	public ActorCompositeKey(){
		
	}
	
	public ActorCompositeKey(String actorName, String movieCount, String yearName){
		this.actorName=actorName;
		this.movieCount=movieCount;
		this.yearName=yearName;
	}
	
	public void clear(){
		this.movieCount="";
		this.actorName="";
		this.yearName="";
	}
	@Override
	public String toString(){
		StringBuilder strBldr=new StringBuilder();
		return strBldr.append("actorName=").append(actorName).append("count=").append(movieCount).append("YearName=").append(yearName).toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException{
		actorName=WritableUtils.readString(in);
		movieCount=WritableUtils.readString(in);
		yearName = WritableUtils.readString(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException{
		WritableUtils.writeString(out, actorName);
		WritableUtils.writeString(out, movieCount);
		WritableUtils.writeString(out, yearName);
	}
	 
	@Override
	public int compareTo(ActorCompositeKey o) {
	 
		int result = actorName.compareTo(o.actorName);
		if(0 == result){
			result = movieCount.compareTo(o.movieCount);
		}
		if(0 == result){
			result=yearName.compareTo(o.yearName);
		}
		return result;
		
	}
	public String getActorName() {
		return actorName;
	}
	public void setActorName(String actorName) {
		this.actorName = actorName;
	}
	public String getMovieCount() {
		return movieCount;
	}
	public void setMovieCount(String movieCount) {
		this.movieCount = movieCount;
	}

	

	
}
