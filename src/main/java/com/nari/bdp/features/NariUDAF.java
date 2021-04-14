package com.nari.bdp.features;

import com.nari.bdp.features.constant.DevEnum;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.lazy.LazyTimestamp;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author ：zhaoy
 * @date ：Created in 2021/4/12 20:41
 * @description：
 * @modified By：
 * @version: 1.0
 */
public class NariUDAF extends UDAF {

	public static class Evaluator implements UDAFEvaluator {
		private final List<String> measTypes = Arrays.asList("13032001", "81302001", "81322001");
		// 设置成员变量，存储每个统计范围内的总记录数
		Object[] calcResults;

		@Override
		public void init() {
			calcResults = new Object[10];
		}


		//map阶段，返回值为boolean类型，当为true则程序继续执行，当为false则程序退出
		public boolean iterate(Object[] args) {
			if (args.length != 63) {
				return true;
			}
			calcResults[0] =args[1];//后面需要提取日期
			calculatePeakValue(args, calcResults);
			return true;
		}

		public Object[] terminatePartial() {
			return calcResults;
		}

		// reduce 阶段，用于逐个迭代处理map当中每个不同key对应的 terminatePartial的结果
		public boolean merge(Object[] calcResults) {
//			calcResults[1]；//= max;
//			calcResults[2]；//= min;
//			calcResults[3]；//= maxValueTime;
//			calcResults[4]；//= minValueTime;
//			calcResults[5]；//= average;
//			calcResults[6]；//= integralPower;
//			this.calcResults[1] = Math.max((Double)this.calcResults[1],(Double)calcResults[1]);
			// max_value
			Double max = (Double)calcResults[1];
			// min_value
			Double min = (Double)calcResults[1];
			// max_value_time
			Timestamp maxValueTime = (Timestamp)calcResults[3];
			// min_value_time
			Timestamp minValueTime = (Timestamp)calcResults[4];
			Timestamp createTime = (Timestamp)calcResults[9];

			Double doubleI =(Double)calcResults[1];
			String aclineendId =(String)calcResults[7];
			String measType =(String)calcResults[8];
			// 不取绝对值的情况
			if (Objects.nonNull(aclineendId) && aclineendId.startsWith(DevEnum.PWRGRID.getCode()) && measTypes.contains(measType)) {
				// 求最大值
				if (Objects.isNull(max)) {
					max = doubleI;
					// 最大值时间
					if (Objects.nonNull(max)) {
						maxValueTime = new Timestamp(createTime.getTime() );
					}
				} else if (Objects.nonNull(doubleI) && max.compareTo(doubleI) < 0) {
					max = doubleI;
					maxValueTime = new Timestamp(createTime.getTime() );
				}

				// 求最小值
				if (Objects.isNull(min)) {
					min = doubleI;
					// 最小值时间
					if (Objects.nonNull(min)) {
						minValueTime = new Timestamp(createTime.getTime() );
					}
				} else if (Objects.nonNull(doubleI) && min.compareTo(doubleI) > 0) {
					min = doubleI;
					minValueTime = new Timestamp(createTime.getTime() );
				}
				// 取绝对值的情况
			} else {
				// 求最大值
				if (Objects.isNull(max)) {
					max = doubleI;
					// 最大值时间
					if (Objects.nonNull(max)) {
						maxValueTime = new Timestamp(createTime.getTime() );
					}
				} else if (Objects.nonNull(doubleI) && Math.abs(max) < (Math.abs(doubleI))) {
					max = doubleI;
					maxValueTime = new Timestamp(createTime.getTime() );
				}

				// 求最小值
				if (Objects.isNull(min)) {
					min = doubleI;
					// 最小值时间
					if (Objects.nonNull(min)) {
						minValueTime = new Timestamp(createTime.getTime() );
					}
				} else if (Objects.nonNull(doubleI) && Math.abs(min) > (Math.abs(doubleI))) {
					min = doubleI;
					minValueTime = new Timestamp(createTime.getTime() );
				}
			}
			return true;
		}


		// 处理merge计算完成后的结果，即对merge完成后的结果做最后的业务处理
		public Object[] terminate() {
//			ArrayList<Object> calcResults = new ArrayList<>();

			return calcResults;
		}

		private void calculatePeakValue(Object[] args, Object[] calcResults) {
			// max_value
			Double max = null;
			// min_value
			Double min = null;
			// max_value_time
			Timestamp maxValueTime = null;
			// min_value_time
			Timestamp minValueTime = null;

			// 如果 create_time 为空，取 1970-01-01 08:00:00
			Timestamp createTime = args[1] == null ? new Timestamp(0) : ((LazyTimestamp) args[1]).getWritableObject().getTimestamp();
			String aclineendId = args[0] == null ? null : args[0].toString();
			String measType = args[2] == null ? null : args[2].toString();

			Object[] deviceValues = Arrays.copyOfRange(args, 2, 62);

			// 计算极值
			for (int i = 0; i < deviceValues.length; i++) {
				Double doubleI = deviceValues[i] == null ? null : ((LazyDouble) deviceValues[i]).getWritableObject().get();

				// 不取绝对值的情况
				if (Objects.nonNull(aclineendId) && aclineendId.startsWith(DevEnum.PWRGRID.getCode()) && measTypes.contains(measType)) {
					// 求最大值
					if (Objects.isNull(max)) {
						max = doubleI;
						// 最大值时间
						if (Objects.nonNull(max)) {
							maxValueTime = new Timestamp(createTime.getTime() );
						}
					} else if (Objects.nonNull(doubleI) && max.compareTo(doubleI) < 0) {
						max = doubleI;
						maxValueTime = new Timestamp(createTime.getTime() );
					}

					// 求最小值
					if (Objects.isNull(min)) {
						min = doubleI;
						// 最小值时间
						if (Objects.nonNull(min)) {
							minValueTime = new Timestamp(createTime.getTime() );
						}
					} else if (Objects.nonNull(doubleI) && min.compareTo(doubleI) > 0) {
						min = doubleI;
						minValueTime = new Timestamp(createTime.getTime() );
					}
					// 取绝对值的情况
				} else {
					// 求最大值
					if (Objects.isNull(max)) {
						max = doubleI;
						// 最大值时间
						if (Objects.nonNull(max)) {
							maxValueTime = new Timestamp(createTime.getTime() );
						}
					} else if (Objects.nonNull(doubleI) && Math.abs(max) < (Math.abs(doubleI))) {
						max = doubleI;
						maxValueTime = new Timestamp(createTime.getTime() );
					}

					// 求最小值
					if (Objects.isNull(min)) {
						min = doubleI;
						// 最小值时间
						if (Objects.nonNull(min)) {
							minValueTime = new Timestamp(createTime.getTime() );
						}
					} else if (Objects.nonNull(doubleI) && Math.abs(min) > (Math.abs(doubleI))) {
						min = doubleI;
						minValueTime = new Timestamp(createTime.getTime() );
					}
				}
			}

			// 非空数值个数
			long count = Arrays.stream(deviceValues).filter(Objects::nonNull).count();
			// 非空数值之和
			Double integralPower = Arrays.stream(deviceValues)
					.filter(Objects::nonNull)
					.map(x -> ((LazyDouble) x).getWritableObject().get())
					.reduce((aDouble, aDouble2) -> new BigDecimal(aDouble.toString()).add(new BigDecimal(aDouble2.toString())).doubleValue())
					.orElse(null);

			// 平均值,精确到小数点后四位
			Double average = count == 0 || integralPower == null ? null : Double.parseDouble(String.format("%.4f", integralPower / count));

			calcResults[1] = max;
			calcResults[2] = min;
			calcResults[3] = maxValueTime;
			calcResults[4] = minValueTime;
			calcResults[5] = average;
			calcResults[6] = integralPower;
			calcResults[7] = aclineendId;
			calcResults[8] = measType;
			calcResults[9] = createTime;
		}
	}
}
