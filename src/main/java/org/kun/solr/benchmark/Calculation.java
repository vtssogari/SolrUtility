package org.kun.solr.benchmark;

import java.util.ArrayList;
import java.util.List;

public class Calculation {

	private List<Integer> qtimeList = new ArrayList<Integer>();
	private int totalTime = 0;
	private int count = 0;
	
	public void addQTime(int qtime){
		qtimeList.add(qtime);
		totalTime += qtime;
		count++;
	}
	
	public double getAverageTime(){
		return totalTime/count;		
	}
}
