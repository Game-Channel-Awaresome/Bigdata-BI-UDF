package com.heyijoy.libs.udf;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

public class StringCleaner extends GenericUDF{
	
	private transient ObjectInspectorConverters.Converter[] converters;
	
	public static void main(String[] args) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		String urlstr1 = "%E9%BB%91%E6%9A%97%E5%B8%88%E7%BB%B4%E5%9F%BA";
		String urlstr = "asdas71263%E9%BB%91%E6";
		System.out.println(URLDecoder.decode(urlstr,"UTF-8"));
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		// TODO Auto-generated method stub
		if (arguments[0].get()==null){
			return null;
		}
		
		String tobecleanstr = arguments[0].get().toString();
		String decodestr;
		
		try {
			decodestr = URLDecoder.decode(tobecleanstr,"UTF-8");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			decodestr = tobecleanstr;
		}
		
		return new Text(decodestr);
		
	}

	@Override
	public String getDisplayString(String[] arg0) {
		// TODO Auto-generated method stub
		return "string_cleaner";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		// TODO Auto-generated method stub
		if (arguments.length != 1) {
			throw new UDFArgumentLengthException("string_clean founction need arg like this:string_clean(toBeCleanString)");
		}
		return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}

}
