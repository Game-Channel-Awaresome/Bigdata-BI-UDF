package com.heyijoy.libs.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;

@SuppressWarnings("deprecation")
public class FixDataEntry extends UDAF {
	public static class UDAFFixDataEvaluator extends FixDataImpl.FixDataEvaluator {}
}
