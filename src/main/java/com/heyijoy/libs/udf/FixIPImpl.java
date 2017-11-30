package com.heyijoy.libs.udf;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.json.JSONException;
import org.json.JSONObject;

@SuppressWarnings("deprecation")
public class FixIPImpl extends AbstractGenericUDAFResolver{
	
	static ArrayList<String> severevent = new ArrayList<String>(Arrays.asList("user login","user pay"));
	static ArrayList<String> sdkevent = new ArrayList<String>(Arrays.asList("create role","select server","enter game","level up","exit game"));
	
	public static class State {
		String correctip;			//上一条有效ip
		ArrayList<String> context;	//待修复的记录原文字串列表
		ArrayList<String> result;	//修复后的记录字串列表
	}
	
	
	public static class FixIPEvaluator implements UDAFEvaluator{
		
		private State state;
		
		public String getip(String eventname,String context,State state){
			//通过常规方式获取ip，获取不到则返回null
			JSONObject contextjsonobj;
			try {
				contextjsonobj = new JSONObject(context);
			} catch (JSONException e0) {
				return null;
			}                         
			
			if (severevent.contains(eventname)){
				//是sdk后端报送的数据
				try{
					state.correctip = ((JSONObject) contextjsonobj.get("context")).get("ip").toString();
					return state.correctip;
				}catch (JSONException e1){
					return null;
				}
			}else if(sdkevent.contains(eventname)){
				//是sdk前端报送的数据
				try {
					state.correctip = ((JSONObject) contextjsonobj.get("matrix_sdk_context")).get("ip").toString();
					return state.correctip;
				} catch (JSONException e3) {
					return null;
				}
			}else{
				//合一自有sdk后端报送的数据，不做考虑
			}
			return null;
		}
		
		public FixIPEvaluator() {
	    	super();
	    	state = new State();
	    	init();
	    }
		
		public void init() {
			// TODO Auto-generated method stub
			state.correctip = null;
			state.context = new ArrayList<String>();
			state.result = new ArrayList<String>();
		}
		
		public boolean iterate(String servertime, String contextjsonstr, String tobefix_eventname) throws ParseException, JSONException {
			JSONObject contextjsonobj = new JSONObject(contextjsonstr);
			String record_eventname = contextjsonobj.get("event").toString();
			
			if (!(severevent.contains(record_eventname) || sdkevent.contains(record_eventname))){
				//如果本条记录不在处理范围内，原样返回
				state.result.add(servertime+"\t"+contextjsonstr);
				return true;
			}
			
			try{
				//兼容了ip位置不正确时的情况
				String errorip = contextjsonobj.get("ip").toString();
				contextjsonobj.remove("ip");
				((JSONObject) contextjsonobj.get("context")).put("ip", errorip);
				String resultstr = contextjsonobj.toString();
				state.result.add(servertime+"\t"+resultstr);
				state.correctip = errorip;
				return true;
			}catch (Exception e5){
			}
			
			if (record_eventname.equals(tobefix_eventname) || tobefix_eventname.equals("all")){
				//如果本条记录事件名与输入的待修复事件相等，则用上一条有效ip修复它
				if (getip(record_eventname,contextjsonstr,state)==null){
					state.context.add(servertime+"\t"+contextjsonstr);
				}else if (getip(record_eventname,contextjsonstr,state).equals("")){
					
					//如果获取到ip为空字串，则删掉ip字段并放入待修复列表
					if (severevent.contains(record_eventname)){
						((JSONObject) contextjsonobj.get("context")).remove("ip");
					}else if(sdkevent.contains(record_eventname)){
						((JSONObject) contextjsonobj.get("matrix_sdk_context")).remove("ip");
					}else{}
					state.context.add(servertime+"\t"+contextjsonobj.toString());
					
				}else{
					//能够获取到正确ip
					if (tobefix_eventname.equals("all")){
						state.correctip = getip(record_eventname,contextjsonstr,state);
					}else{
						//获取到了待修复事件的正常ip，不作为正确ip记录
					}
					state.result.add(servertime+"\t"+contextjsonstr);
				}
				
				//如果此时已有有效ip存在，则用ip将列表中所有待修复的记录修复
				if (state.correctip != null){
					if (state.context.size() == 0){
					}else{
						ArrayList<String> tobedelete =new ArrayList<String>();
						for(String tobefixed_context:state.context){
							String[] tobefixed_split = tobefixed_context.split("\t");
							JSONObject tobefixed_jsonobj = new JSONObject(tobefixed_split[1]);
							if (severevent.contains(record_eventname)){
								((JSONObject) tobefixed_jsonobj.get("context")).put("ip", state.correctip);
							}else if(sdkevent.contains(record_eventname)){
								((JSONObject) tobefixed_jsonobj.get("matrix_sdk_context")).put("ip", state.correctip);
							}else{}
							
							state.result.add(tobefixed_split[0]+"\t"+tobefixed_jsonobj.toString());
							tobedelete.add(tobefixed_context);
							}
						for (String deletestr:tobedelete) {
							state.context.remove(deletestr);
							}
						}
				}else{}
				
			}else{
				//如果本条记录不是待修复事件，则尝试能否取出它的有效ip
				if (getip(record_eventname,contextjsonstr,state)==null || getip(record_eventname,contextjsonstr,state).equals("")){}else{
					state.correctip = getip(record_eventname,contextjsonstr,state);
				}
				state.result.add(servertime+"\t"+contextjsonstr);
			}
			return true;
		}
		
		public boolean merge(ArrayList<String> o) {
			if (o != null) {
				this.state.result.addAll(o);
			}
			return true;
		}

	    public ArrayList<String> terminatePartial() {
	    	return this.state.result.size() == 0 ? state.context : state.result;
	    }

	    public ArrayList<String> terminate() {
	    	return this.state.result.size() == 0 ? state.context : state.result;
	    }
	    
		
	}
	
}
