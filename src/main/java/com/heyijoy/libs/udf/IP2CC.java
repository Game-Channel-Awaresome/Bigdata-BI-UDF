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
import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.nullCondition_return;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.ivy.osgi.updatesite.xml.EclipseFeature;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Subdivision;

public class IP2CC extends GenericUDF{
	
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
	
	public Text evaluate(Text ip) {
		String r = null;
		try{
			DatabaseReader reader = getDatabaseReader();
			InetAddress ipAddress = InetAddress.getByName(ip.toString());
			CityResponse response = reader.city(ipAddress);
			Country country = response.getCountry();
			String countryname = country.getNames().get("zh-CN");
	
			Subdivision subdivision = response.getMostSpecificSubdivision();
			String provincename = subdivision.getNames().get("zh-CN");
	
			City city = response.getCity();
			String cityname = city.getNames().get("zh-CN");
			
			r = String.join("\t", countryname, provincename, cityname);
		} catch (Exception e){
//			r =  String.join("\t", e.toString(), "", "");
			r = String.join("\t", "ip error", "", "");
		}
		return new Text(r);
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if (arguments == null){
			return null;
		}else if(arguments[0] == null){
			return null;
		}else if(arguments[0].get() == null){
			return null;
		}
		Text ip = new Text(arguments[0].get().toString());
		return this.evaluate(ip);
	}

	@Override
	public String getDisplayString(String[] arguments) {
		return "ip2cc";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length != 1) {
			throw new UDFArgumentLengthException("only accept 1 argument");
		}
		return  PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}
}
