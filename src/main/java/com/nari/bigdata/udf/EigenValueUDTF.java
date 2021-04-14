package com.nari.bigdata.udf;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class EigenValueUDTF  extends GenericUDTF {

    private List<Object> dataList = new ArrayList<Object>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //定义输出数据的列名
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("id");
        fieldNames.add("datasource_id");
        fieldNames.add("meas_type");
        fieldNames.add("create_date");

        fieldNames.add("max_value");
        fieldNames.add("max_value_time");
        fieldNames.add("min_value");
        fieldNames.add("min_value_time");
        fieldNames.add("average_value");
        fieldNames.add("integral_power");
        //定义输出数据的类型
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    public void process(Object[] args) throws HiveException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String id = args[0].toString();
        String datasource_id = args[1].toString();
        String meas_type = args[2].toString();
        String create_date = args[3].toString();
        try {
            create_date = df.format(df.parse(create_date));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        float max = -10000f;
        float min = 10000f;
        float integral_power = 0.0f;
        if(args[4] != null){
            max = Float.parseFloat(args[4].toString());
            min = Float.parseFloat(args[4].toString());
        }
        for (int i = 4; i < args.length; i++) {
            if(args[i] == null){
                continue;
            }
            float decValue = Float.parseFloat(args[i].toString());
            if(max < decValue){
                max = decValue;
            }
            if(min > decValue){
                min = decValue;
            }
            integral_power = integral_power + decValue;
        }
        float average = integral_power / (args.length - 1);
        String max_value_time = args[3].toString();
        String min_value_time = args[3].toString();
        //将数据放置到集合
        dataList.clear();
        dataList.add(id);
        dataList.add(datasource_id);
        dataList.add(meas_type);
        dataList.add(create_date);

        dataList.add(HiveDecimal.create(max));
        dataList.add(max_value_time);
        dataList.add(HiveDecimal.create(min));
        dataList.add(min_value_time);
        dataList.add(HiveDecimal.create(average));
        dataList.add(HiveDecimal.create(integral_power));
        //写出数据操作
        forward(dataList);
    }

    @Override
    public void close() throws HiveException {

    }
}
