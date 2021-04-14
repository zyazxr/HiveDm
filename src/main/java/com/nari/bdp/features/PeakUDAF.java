//package com.nari.bdp.features;
//
//import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
//import org.apache.hadoop.hive.ql.metadata.HiveException;
//import org.apache.hadoop.hive.ql.parse.SemanticException;
//import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
//import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
//import org.apache.hadoop.hive.serde2.objectinspector.*;
//import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
//import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
//import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
//import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
//import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
//import org.apache.hadoop.io.LongWritable;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//
///**
// * @author ：zhaoy
// * @date ：Created in 2021/4/13 16:53
// * @description：
// * @modified By：
// * @version: 1.0
// */
//public class PeakUDAF  extends AbstractGenericUDAFResolver {
//	@Override
//	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
//			throws SemanticException {
//		if (parameters.length != 65) {
//			throw new UDFArgumentTypeException(parameters.length - 1,
//					"Exactly 65 argument is expected.");
//		}
//
//		if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
//			throw new UDFArgumentTypeException(0,
//					"Only primitive type arguments are accepted but "
//							+ parameters[0].getTypeName() + " is passed.");
//		}
//		return new GenericUDAFPeakEvaluator();
//
//	}
//
//
//	public static class GenericUDAFPeakEvaluator extends GenericUDAFEvaluator {
//
//		// input For iterate()
//
//		PrimitiveObjectInspector aclineend_id;
//		PrimitiveObjectInspector create_time;
//		PrimitiveObjectInspector datasource_id;
//		PrimitiveObjectInspector meas_type;
//		ObjectInspector[] value;
//
//
//		// output For terminatePartial()
//		Object[] partialAggregationResult;
//
//		// input For merge()
//		StructObjectInspector soi;
//		StructField countField;
//		StructField sumField;
//		LongObjectInspector countFieldOI;
//		LongObjectInspector sumFieldOI;
//
//		// output For terminate()
//		LongWritable fullAggregationResult;
//
//		@Override
//		public ObjectInspector init(Mode mode, ObjectInspector[] parameters)
//				throws HiveException {
//			super.init(mode, parameters);
//
//			// init input
//			// Mode.PARTIAL1 || mode == Mode.COMPLETE
//			// input:original, method:iterate()
//			if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
//				aclineend_id = (PrimitiveObjectInspector) parameters[0];
//				create_time = (PrimitiveObjectInspector) parameters[1];
//				datasource_id = (PrimitiveObjectInspector) parameters[1];
//				meas_type = (PrimitiveObjectInspector) parameters[1];
//				value=  Arrays.copyOfRange(parameters, 5, 65);
//			}
//			// Mode.PARTIAL2 || Mode.FINAL
//			// input:partial aggregation, method:merge()
//			else {
//				//部分数据作为输入参数时，用到的struct的OI实例，指定输入数据类型，用于解析数据
//				soi = (StructObjectInspector) parameters[0];
//				countField = soi.getStructFieldRef("count");
//				sumField = soi.getStructFieldRef("sum");
//				//数组中的每个数据，需要其各自的基本类型OI实例解析
//				countFieldOI = (LongObjectInspector) countField.getFieldObjectInspector();
//				sumFieldOI = (LongObjectInspector) sumField.getFieldObjectInspector();
//			}
//
//			// init output
//			// Mode.PARTIAL1 || mode == Mode.PARTIAL2
//			// output:partial aggregation, method:terminatePartial()
//			if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
//				partialAggregationResult = new Object[2];
//				partialAggregationResult[0] = new LongWritable(0);
//				partialAggregationResult[1] = new LongWritable(0);
//				/*
//				 * 构造Struct的OI实例，用于设定聚合结果数组的类型
//				 * 需要字段名List和字段类型List作为参数来构造
//				 */
//				ArrayList<String> fname = new ArrayList<String>();
//				fname.add("count");
//				fname.add("sum");
//				ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
//				//注：此处的两个OI类型 描述的是 partialResult[] 的两个类型，故需一致
//				foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
//				foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
//				return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
//			}
//			// Mode.COMPLETE || Mode. FINAL
//			// output:full aggregation, method:terminate()
//			else {
//				//FINAL COMPLETE 最终聚合结果为一个数值，并用基本类型OI设定其类型
//				fullAggregationResult = new LongWritable(0);
//				return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
//			}
//		}
//
//		/*
//		 * 聚合数据缓存存储结构
//		 */
//		static class AverageAgg implements AggregationBuffer {
//			long count;
//			long sum;
//		}
//
//		@Override
//		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
//			WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg result = new WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg();
//			reset(result);
//			return result;
//		}
//
//		@Override
//		public void reset(AggregationBuffer agg) throws HiveException {
//			WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg myagg = (WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg) agg;
//			myagg.count = 0;
//			myagg.sum = 0;
//		}
//
//		/*
//		 * 遍历原始数据(将一行数据（Object[] parameters）放入聚合buffer中)
//		 * input: original
//		 */
//		@Override
//		public void iterate(AggregationBuffer agg, Object[] parameters)
//				throws HiveException {
//			Object p1 = parameters[0];
//			Object p2 = parameters[1];
//			if (p1 != null && p2 != null) {
//				WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg myagg = (WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg) agg;
//				try {
//					long avg = PrimitiveObjectInspectorUtils.getLong(p1, avgOriginalInputOI);
//					long count = PrimitiveObjectInspectorUtils.getLong(p2, weightOriginalInputOI);
//					myagg.count += count;
//					myagg.sum += avg*count;
//				} catch (NumberFormatException e) {
//					throw new HiveException("NumberFormatException: get value failed");
//				}
//			}
//		}
//
//		/*
//		 * 得出部分聚合结果
//		 * output: partial aggregation
//		 */
//		@Override
//		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
//			WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg myagg = (WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg) agg;
//			((LongWritable) partialAggregationResult[0]).set(myagg.count);
//			((LongWritable) partialAggregationResult[1]).set(myagg.sum);
//			return partialAggregationResult;
//		}
//
//		/*
//		 * 合并部分聚合结果(注：Object[] 是 Object 的子类，此处 partial 为 Object[]数组)
//		 * input: partial aggregation
//		 */
//		@Override
//		public void merge(AggregationBuffer agg, Object partial)
//				throws HiveException {
//			if (partial != null) {
//				WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg myagg = (WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg) agg;
//				//通过StandardStructObjectInspector实例，分解出 partial 数组元素值
//				Object partialCount = soi.getStructFieldData(partial, countField);
//				Object partialSum = soi.getStructFieldData(partial, sumField);
//				//通过基本数据类型的OI实例解析Object的值
//				myagg.count += countFieldOI.get(partialCount);
//				myagg.sum += sumFieldOI.get(partialSum);
//			}
//		}
//
//		/*
//		 * 得出最终聚合结果
//		 * output: full aggregation
//		 */
//		@Override
//		public Object terminate(AggregationBuffer agg) throws HiveException {
//			WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg myagg = (WeightedAverage.GenericUDAFAverageEvaluator.AverageAgg) agg;
//			if (myagg.count == 0) {
//				return null;
//			} else {
//				fullAggregationResult.set(myagg.sum / myagg.count);
//				return fullAggregationResult;
//			}
//		}
//	}
//
//}