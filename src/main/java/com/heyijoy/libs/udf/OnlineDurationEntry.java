package com.heyijoy.libs.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;

@SuppressWarnings("deprecation")
public class OnlineDurationEntry extends UDAF {
	public static class UDAFOnlineDurationEvaluator extends OnlineDurationImpl.OnlineDurationEvaluator {}
}
