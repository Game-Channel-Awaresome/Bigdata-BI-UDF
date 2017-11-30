package com.heyijoy.libs.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.joda.time.DateTime;



@SuppressWarnings("deprecation")
public class OnlineDurationImpl extends AbstractGenericUDAFResolver{
	
	
	public static class State {
		long dialogNum;
		long onlineDuration;
		String result;
		ArrayList<Long> sortRecordTime;
		long lastRecordTS;
		long limitSecond;
	}
	
	
	public static class OnlineDurationEvaluator implements UDAFEvaluator{
		
		private State state;
		
		public boolean isNumeric(String str){
			for (int i = str.length();--i>=0;){
				if (!Character.isDigit(str.charAt(i))){
					return false;
				}
			}
			return true;
		}
		
		public OnlineDurationEvaluator() {
	    	super();
	    	state = new State();
	    	init();
	    }
		
		public void init() {
			state.dialogNum = 0;
			state.onlineDuration = 0;
			state.result = "";
			state.sortRecordTime = new ArrayList<Long>();
			state.limitSecond = (long) 0;
			state.lastRecordTS = (long) 0;
		}
		
		public boolean iterate(String recordTime,String limitSecond){
			
			state.limitSecond = Long.parseLong(limitSecond);
			
			DateTime recordtimeobj;
			
			//分为时间戳和ISO8601两种情况
			if (isNumeric(recordTime) == true) {
				recordtimeobj = new DateTime(Long.parseLong(recordTime)*1000);
			}else{
				recordtimeobj = new DateTime(DateTime.parse(recordTime));
			}
			
			state.sortRecordTime.add(recordtimeobj.getMillis());
			
			return true;
		}
		
		public boolean merge(State state) {
			if (state != null) {
				this.state.sortRecordTime.addAll(state.sortRecordTime);
				this.state.limitSecond = state.limitSecond;
			}
			return true;
		}

	    public State terminatePartial() {
	    	return this.state.sortRecordTime.size() == 0 ? null : state;
	    }

	    public String terminate() {
	    	this.state.sortRecordTime.sort(null);
	    	for (long recordTS : this.state.sortRecordTime) {
	    		//如果与上条记录超过时长则会话数+1,没超过则在线时长+秒数
				if (this.state.lastRecordTS != 0){
					long seconds = Math.abs(this.state.lastRecordTS-recordTS);
					if ((seconds/1000) > this.state.limitSecond){
						this.state.dialogNum++;
					}else{
						this.state.onlineDuration += seconds/1000;
					}
				}else{
					this.state.dialogNum++;
				}
				
				this.state.lastRecordTS = recordTS;
				
			}
	    	this.state.result = Long.toString(this.state.dialogNum) + "\t" + Long.toString(this.state.onlineDuration);
	    	return this.state.result.length() == 0 ? null : this.state.result;
	    }
	    
		
	}
	
}
