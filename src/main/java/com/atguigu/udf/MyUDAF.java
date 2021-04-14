package com.atguigu.udf;

import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

/**
 * @author ：zhaoy
 * @date ：Created in 2021/4/12 15:27
 * @description：
 * @modified By：
 * @version: 1.0
 */
public class MyUDAF extends GenericUDAFEvaluator {

	private PrimitiveObjectInspector inputOI;
	private HiveDecimalWritable result;

	@Override
	public AggregationBuffer getNewAggregationBuffer() throws HiveException {
		return null;
	}

	@Override
	public void reset(AggregationBuffer agg) throws HiveException {

	}

	@Override
	public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {

	}

	@Override
	public Object terminatePartial(AggregationBuffer agg) throws HiveException {
		return null;
	}

	@Override
	public void merge(AggregationBuffer agg, Object partial) throws HiveException {

	}

	@Override
	public Object terminate(AggregationBuffer agg) throws HiveException {
		return null;
	}
}
