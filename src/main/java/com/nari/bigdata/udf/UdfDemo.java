package com.nari.bigdata.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class UdfDemo extends UDF {
    public int evaluate (int data) {

        return data + 2;
    }

    public int evaluate (int data1, int data2) {

        return data1 + data2 + 2;
    }
}
