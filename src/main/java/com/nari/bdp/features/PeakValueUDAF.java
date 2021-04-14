package com.nari.bdp.features;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.util.ArrayList;

/**
 * @author ：zhaoy
 * @date ：Created in 2021/4/12 16:02
 * @description：
 * @modified By：
 * @version: 1.0
 */
public class PeakValueUDAF extends AbstractGenericUDAFResolver {
	public static Logger logger = Logger.getLogger(PeakValueUDAF.class);

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {
		if (parameters.length != 65) {
			throw new UDFArgumentTypeException(parameters.length - 1,
					"Exactly 65 argument is expected.");
		}
		return new GenericUDAFAverageEvaluator();
	}

	public static class GenericUDAFAverageEvaluator extends GenericUDAFEvaluator {

		// input For iterate()
		PrimitiveObjectInspector aclineend_id;
		PrimitiveObjectInspector create_time;
		PrimitiveObjectInspector datasource_id;
		PrimitiveObjectInspector meas_type;

		// output For terminatePartial()
		Object[] partialAggregationResult;

		// input For merge()
		StructObjectInspector soi;
		StructField countField;
		StructField sumField;
		LongObjectInspector countFieldOI;
		LongObjectInspector sumFieldOI;

		// output For terminate()
		Object[]  fullAggregationResult;

		@Override
		public ObjectInspector init(Mode mode, ObjectInspector[] parameters)
				throws HiveException {
			super.init(mode, parameters);

			// init input
			// Mode.PARTIAL1 || mode == Mode.COMPLETE
			// input:original, method:iterate()
			if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
				aclineend_id = (PrimitiveObjectInspector) parameters[0];
				create_time = (PrimitiveObjectInspector) parameters[1];
				datasource_id = (PrimitiveObjectInspector) parameters[2];
				meas_type = (PrimitiveObjectInspector) parameters[3];
			}
			// Mode.PARTIAL2 || Mode.FINAL
			// input:partial aggregation, method:merge()
			else {
				//部分数据作为输入参数时，用到的struct的OI实例，指定输入数据类型，用于解析数据
				soi = (StructObjectInspector) parameters[0];
				countField = soi.getStructFieldRef("count");
				sumField = soi.getStructFieldRef("sum");
				sumField = soi.getStructFieldRef("sum");
				sumField = soi.getStructFieldRef("sum");
				sumField = soi.getStructFieldRef("sum");
				sumField = soi.getStructFieldRef("sum");
				sumField = soi.getStructFieldRef("sum");
				sumField = soi.getStructFieldRef("sum");
				sumField = soi.getStructFieldRef("sum");
				//数组中的每个数据，需要其各自的基本类型OI实例解析
				countFieldOI = (LongObjectInspector) countField.getFieldObjectInspector();
				sumFieldOI = (LongObjectInspector) sumField.getFieldObjectInspector();
			}

			// init output
			// Mode.PARTIAL1 || mode == Mode.PARTIAL2
			// output:partial aggregation, method:terminatePartial()
			if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
				partialAggregationResult = new Object[12];
//				partialAggregationResult[0] = new LongWritable(0);
//				partialAggregationResult[1] = new LongWritable(0);
				/*
				 * 构造Struct的OI实例，用于设定聚合结果数组的类型
				 * 需要字段名List和字段类型List作为参数来构造
				 */
				ArrayList<String> fname = new ArrayList<String>();
				fname.add("create_date");
				fname.add("max_value");
				fname.add("min_value");
				fname.add("max_value_time");
				fname.add("min_value_time");
				fname.add("average");
				fname.add("integral_power");
				ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
				//注：此处的两个OI类型 描述的是 partialResult[] 的两个类型，故需一致
				foi.add(PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
				foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
				foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
				foi.add(PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
				foi.add(PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
				foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
				foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
				return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
			}
			// Mode.COMPLETE || Mode. FINAL
			// output:full aggregation, method:terminate()
			else {
				//FINAL COMPLETE 最终聚合结果为一个数值，并用基本类型OI设定其类型
				fullAggregationResult =  new Object[12];
				return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(P);
			}
		}

		/*
		 * 聚合数据缓存存储结构
		 */
		static class AverageAgg implements AggregationBuffer {
			long count;
			long sum;
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg result = new WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg();
			reset(result);
			return result;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg myagg = (WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg) agg;
			myagg.count = 0;
			myagg.sum = 0;
		}

		/*
		 * 遍历原始数据(将一行数据（Object[] parameters）放入聚合buffer中)
		 * input: original
		 */
		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters)
				throws HiveException {
			Object p1 = parameters[0];
			Object p2 = parameters[1];
			if (p1 != null && p2 != null) {
				WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg myagg = (WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg) agg;
				try {
					long avg = PrimitiveObjectInspectorUtils.getLong(p1, avgOriginalInputOI);
					long count = PrimitiveObjectInspectorUtils.getLong(p2, weightOriginalInputOI);
					myagg.count += count;
					myagg.sum += avg * count;
				} catch (NumberFormatException e) {
					throw new HiveException("NumberFormatException: get value failed");
				}
			}
		}

		/*
		 * 得出部分聚合结果
		 * output: partial aggregation
		 */
		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg myagg = (WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg) agg;
			((LongWritable) partialAggregationResult[0]).set(myagg.count);
			((LongWritable) partialAggregationResult[1]).set(myagg.sum);
			return partialAggregationResult;
		}

		/*
		 * 合并部分聚合结果(注：Object[] 是 Object 的子类，此处 partial 为 Object[]数组)
		 * input: partial aggregation
		 */
		@Override
		public void merge(AggregationBuffer agg, Object partial)
				throws HiveException {
			if (partial != null) {
				WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg myagg = (WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg) agg;
				//通过StandardStructObjectInspector实例，分解出 partial 数组元素值
				Object partialCount = soi.getStructFieldData(partial, countField);
				Object partialSum = soi.getStructFieldData(partial, sumField);
				//通过基本数据类型的OI实例解析Object的值
				myagg.count += countFieldOI.get(partialCount);
				myagg.sum += sumFieldOI.get(partialSum);
			}
		}

		/*
		 * 得出最终聚合结果
		 * output: full aggregation
		 */
		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg myagg = (WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg) agg;
			if (myagg.count == 0) {
				return null;
			} else {
				fullAggregationResult.set(myagg.sum / myagg.count);
				return fullAggregationResult;
			}
		}
	}
}
