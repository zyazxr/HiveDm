package com.nari.bdp.features;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * @author ：zhaoy
 * @date ：Created in 2021/4/14 9:44
 * @description：
 * @modified By：
 * @version: 1.0
 */
public class MyPeakUDAF extends AbstractGenericUDAFResolver {


	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {
		// Type-checking goes here! 参数校验
		if (parameters.length != 65) {
			throw new UDFArgumentTypeException(parameters.length - 1,
					"Exactly one argument is expected.");
		}
		return new GenericUDAFPeak();
	}

	public static class GenericUDAFPeak extends GenericUDAFEvaluator {
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
		// UDAF logic goes here!
	}
}