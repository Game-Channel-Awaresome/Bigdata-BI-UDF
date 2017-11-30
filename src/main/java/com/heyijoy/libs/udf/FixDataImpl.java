package com.heyijoy.libs.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;

public class FixDataImpl extends AbstractGenericUDAFResolver {
	
	static int[] daypercent = new int[]{95,90,80,70,60,60,60,30,20,20};
	
	public static class State {
		String value;
		ArrayList<String> dataList = new ArrayList<String>();
	}
	
	public static class FixDataEvaluator implements UDAFEvaluator {
		
		private State state;
		
		public boolean toBeFix(int percent){
			int x=(int)(Math.random()*100);
			if (x <= percent){
				return true;
			}else {
				return false;
			}
		}
		
		public FixDataEvaluator() {
			super();
			state = new State();
			init();
		}

		public void init() {
			state.value = "";
			state.dataList = new ArrayList<String>();
		}
		
		public boolean iterate(String data){
			this.state.dataList.add(data);
			return true;
		}
		
		public State terminatePartial() {
			return this.state.dataList.size() == 0 ? null : state;
		}
		
		public boolean merge(State o) {
			if (o != null) {
				this.state.dataList.addAll(o.dataList);
			}
			return true;
		}
		
		public State terminate() {
			return this.state.dataList.size() == 0 ? null : this.state;
		}
		
	}
}