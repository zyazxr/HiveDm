//package com.ali.udaf;
//
///**
// * @author ：zhaoy
// * @date ：Created in 2021/4/12 15:30
// * @description：
// * @modified By：
// * @version: 1.0
// */
///*
// * @param <T> UDAF的输出结果的类型。
// * @param <ACC> UDAF的accumulator的类型。accumulator是UDAF计算中用来存放计算中间结果的数据类型。您可以需要根据需要自行设计每个UDAF的accumulator。
// */
//public abstract class AggregateFunction<T, ACC> extends UserDefinedFunction {
//	/*
//	 * 初始化AggregateFunction的accumulator。
//	 * 系统在进行第一个aggregate计算之前，调用一次此方法。
//	 */
//	public ACC createAccumulator()；
//	/*
//	 * 系统在每次aggregate计算完成后，调用此方法。
//	 */
//	public T getValue(ACC accumulator)；
//}