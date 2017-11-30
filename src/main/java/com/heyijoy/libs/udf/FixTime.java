package com.heyijoy.libs.udf;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Seconds;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Location;

public class FixTime extends GenericUDF{
	
	private static DatabaseReader databaseReader = null;
	static String dbfile = "/matrix/files/share/GeoLite2-City.mmdb";
	static Path path = new Path(dbfile);
	static FileSystem fs = null;
	
	static {
		final Configuration conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	static File database = new File(dbfile);
	
	static DatabaseReader getDatabaseReader() throws IOException {
		
		BufferedInputStream is = new BufferedInputStream(fs.open(path));
		if (databaseReader == null) {
			databaseReader = new DatabaseReader.Builder(is).build();
		}
		return databaseReader;
	}
	
//	public static void main(String args[]) throws IOException, GeoIp2Exception{
//		String time1 = "2017-01-17T15:07:44+08:00";
//		String time2 = "2017-01-17T15:07:44+00:00";
//		
//		DateTime x1 = DateTime.parse(time1);
//		DateTime x2 = DateTime.parse(time2);
//		
//		DateTime x = new DateTime(x2, x1.getZone());
//		System.out.println("================" + x);
//		
//		String time3 = "1484635780";
//		DateTime dt = new DateTime(Integer.parseInt(time3),DateTimeZone.forID("America/Chicago"));
//		System.out.println(dt.getZone());
//		System.out.println(dt.toString("Z"));
//		System.out.println(dt.plusDays(90).toString("E MM/dd/yyyy HH:mm:ss"));
//		evaluate(new Text(time1),new Text("1234781234"),new Text("86400"),new Text("172.16.81.49"));
//	}
	
	static boolean isNumeric(String str){
		for (int i = str.length();--i>=0;){
			if (!Character.isDigit(str.charAt(i))){
				return false;
			}
		}
		return true;
	}
	
	static boolean arguCheck(DeferredObject[] arguments) throws HiveException{
		for(int i=0;i<arguments.length;i++){
			Object argu = arguments[i].get();
			if (argu==null){
				return false;
			}
			String argustr = argu.toString();
			if (argustr.equals("") || argustr.equals("NULL") || argustr.equals("null")){
				return false;
			}
		}
		return true;
	}
	
	public Text evaluate(Text correctTime,Text tobefixTime,Text limitSecond,Text ip) throws IOException, GeoIp2Exception {
		String correctTimeStr = correctTime.toString();
		String tobefixTimeStr = tobefixTime.toString();
		String limitSecondStr = limitSecond.toString();
		String ipTimeZone;
		try{
			DatabaseReader reader = getDatabaseReader();
			InetAddress ipAddress = InetAddress.getByName(ip.toString());
			CityResponse response = reader.city(ipAddress);
			Location location = response.getLocation();
			ipTimeZone = location.getTimeZone();
		} catch (Exception e) {
			ipTimeZone = "Asia/Shanghai";
		}
		
		DateTime tobefixTime_time;
		DateTime correctTime_time;
//		分为时间戳和ISO8601两种情况
		if (tobefixTimeStr.equals("isnull")){
			tobefixTime_time = new DateTime(DateTime.parse(correctTimeStr),DateTimeZone.forID(ipTimeZone));
		}else if (isNumeric(tobefixTimeStr) == true) {
			tobefixTime_time = new DateTime(Long.parseLong(tobefixTimeStr)*1000,DateTimeZone.forID(ipTimeZone));
		}else{
			tobefixTime_time = new DateTime(DateTime.parse(tobefixTimeStr));
		}
		
		 correctTime_time = new DateTime(DateTime.parse(correctTimeStr),tobefixTime_time.getZone());
		
//		将所有时间都转为待修复时间的时区再做计算
		int seconds = Math.abs(Seconds.secondsBetween(correctTime_time, tobefixTime_time).getSeconds());
		
		System.out.println(seconds);
		if (seconds <= Integer.parseInt(limitSecondStr)) {
//			不需要修复，返回
			return new Text(tobefixTime_time.toString("yyyy-MM-dd'T'HH:mm:ssZ"));
		} else {
//			将正确时间转为待转时间的时区并返回
			return new Text(correctTime_time.toString("yyyy-MM-dd'T'HH:mm:ssZ"));
		}
	}
	
	public Text evaluate(Text correctTime,Text tobefixTime,Text limitSecond) throws IOException, GeoIp2Exception {
		String correctTimeStr = correctTime.toString();
		String tobefixTimeStr = tobefixTime.toString();
		String limitSecondStr = limitSecond.toString();
		if (tobefixTimeStr.equals("isnull")){
			tobefixTimeStr = correctTimeStr;
		}
		DateTime tobefixTime_time;
		DateTime correctTime_time;
//		分为时间戳和ISO8601两种情况
		if (isNumeric(tobefixTimeStr) == true) {
			tobefixTime_time = new DateTime(Long.parseLong(tobefixTimeStr)*1000,DateTimeZone.forOffsetHours(8));
		}else{
			tobefixTime_time = new DateTime(DateTime.parse(tobefixTimeStr));
		}
		
		correctTime_time = new DateTime(DateTime.parse(correctTimeStr),tobefixTime_time.getZone());
		
//		将所有时间都转为待修复时间的时区再做计算
		int seconds = Math.abs(Seconds.secondsBetween(correctTime_time, tobefixTime_time).getSeconds());
		
		if (seconds <= Integer.parseInt(limitSecondStr)) {
//			不需要修复，返回待修复时间
			return new Text(tobefixTime_time.toString("yyyy-MM-dd'T'HH:mm:ssZ"));
		} else {
//			需要休息，返回server_time
			return new Text(correctTime_time.toString("yyyy-MM-dd'T'HH:mm:ssZ"));
		}
	}

	
	
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		
		Text tobefixTime;
		Object argu = arguments[1].get();
		if (argu == null){
			tobefixTime = new Text("isnull");
			arguments[1] = arguments[0];
		}else{
			tobefixTime = new Text(arguments[1].get().toString());
		}
		
		if (!arguCheck(arguments)) {
			return null;
		}
		
		if (arguments.length == 3){
			Text correctTime = new Text(arguments[0].get().toString());
			Text limitSecond = new Text(arguments[2].get().toString());
			try {
				return this.evaluate(correctTime,tobefixTime,limitSecond);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		
		if (arguments.length == 4){
			Text correctTime = new Text(arguments[0].get().toString());
			Text limitSecond = new Text(arguments[2].get().toString());
			Text ip = new Text(arguments[3].get().toString());
		
			try {
				return this.evaluate(correctTime,tobefixTime,limitSecond,ip);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				return null;
			} 
		}
		
		
		return new Text("NULL");
	}

	@Override
	public String getDisplayString(String[] arguments) {
		return "fix_time";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length != 4 && arguments.length != 3) {
			throw new UDFArgumentLengthException("fix_time founction need arg as this:(correct_time, tobefixed_time, limit_second, ip)");
		}
		return  PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}
	
}
