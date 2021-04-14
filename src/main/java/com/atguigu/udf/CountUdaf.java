package com.atguigu.udf;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.FunctionParameter;

import java.util.List;

/**
 * @author ：zhaoy
 * @date ：Created in 2021/4/12 15:29
 * @description：
 * @modified By：
 * @version: 1.0
 */
public class CountUdaf implements AggregateFunction {
	@Override
	public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
		return null;
	}

	@Override
	public List<FunctionParameter> getParameters() {
		return null;
	}

	//定义存放count UDAF状态的accumulator的数据的结构。
	public static class CountAccum {
		public long total;
	}

	//初始化count UDAF的accumulator。
	public CountAccum createAccumulator() {
		CountAccum acc = new CountAccum();
		acc.total = 0;
		return acc;
	}

	//getValue提供了如何通过存放状态的accumulator计算count UDAF的结果的方法。
	public Long getValue(CountAccum accumulator) {
		return accumulator.total;
	}

	//accumulate提供了如何根据输入的数据更新count UDAF存放状态的accumulator。
	public void accumulate(CountAccum accumulator, Object iValue) {
		accumulator.total++;
	}

	public void merge(CountAccum accumulator, Iterable<CountAccum> its) {
		for (CountAccum other : its) {
			accumulator.total += other.total;
		}
	}
}