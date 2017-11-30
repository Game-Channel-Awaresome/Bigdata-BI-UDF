package com.heyijoy.libs.udf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.json.JSONException;
import org.json.JSONObject;

import com.sun.mail.smtp.SMTPAddressSucceededException;


public class temp {
	
	static String correctip;			//上一条有效ip
	static ArrayList<String> context;	//待修复的记录原文
	static ArrayList<String> result;
	
	static ArrayList<String> severevent = new ArrayList<String>(Arrays.asList("user login","user pay"));
	static ArrayList<String> sdkevent = new ArrayList<String>(Arrays.asList("create role","select server","enter game","level up","exit game"));

	
	public static String getip(String eventname,String context){
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
				correctip = ((JSONObject) contextjsonobj.get("context")).get("ip").toString();
				return correctip;
			}catch (JSONException e1){
				return null;
			}
		}else if(sdkevent.contains(eventname)){
			//是sdk前端报送的数据
			try {
				correctip = ((JSONObject) contextjsonobj.get("matrix_sdk_context")).get("ip").toString();
				return correctip;
			} catch (JSONException e3) {
				return null;
			}
		}else{
			//合一自有sdk后端报送的数据，不做考虑
		}
		return null;
	}
	
	public static boolean isNumeric(String str){
		for (int i = str.length();--i>=0;){
			if (!Character.isDigit(str.charAt(i))){
				return false;
			}
		}
		return true;
	}
	
	public static void main(String[] args) throws JSONException {
		String recordTime = "1484655419";
		DateTime recordtimeobj;
		if (isNumeric(recordTime) == true) {
			recordtimeobj = new DateTime(Long.parseLong(recordTime)*1000);
		}else{
			recordtimeobj = new DateTime(DateTime.parse(recordTime));
		}
		
		DateTime recordtimeobj2 = new DateTime(Long.parseLong("1484655420")*1000);

		
		System.out.println(recordtimeobj.toString("yyyy-MM-dd'T'HH:mm:ssZ"));
		System.out.println(recordtimeobj2.toString("yyyy-MM-dd'T'HH:mm:ssZ"));
		System.out.println(Math.abs(Seconds.secondsBetween(recordtimeobj, recordtimeobj2).getSeconds()));
		
		System.out.println(recordtimeobj2.getMillis());
		System.out.println(recordtimeobj.getMillis());
		
		String s = "86400";
		long sss = Long.parseLong(s);
		System.out.println(sss);
	}

}
