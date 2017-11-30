package com.heyijoy.libs.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;

@SuppressWarnings("deprecation")
public class FixIPEntry extends UDAF {
	public static class UDAFFixIPEvaluator extends FixIPImpl.FixIPEvaluator {}
}
