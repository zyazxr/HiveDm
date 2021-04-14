package com.nari.bdp.features;

import com.nari.bdp.features.constant.DevEnum;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.lazy.LazyTimestamp;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

/**
 * 极值计算自定义函数,计算每一行中的最大值、最小值.
 * 入参65个，顺序与 {@Description} 严格一致
 */
@Description(name = "peak_value",
    value = "_FUNC_(aclineend_id, create_time, datasource_id, id, meas_type, v00, v01, v02, v03, v04, v05, v06, v07, " +
        "v08, v09,v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20, v21, v22, v23, v24, v25, v26, v27, v28, v29, " +
        "v30, v31, v32,v33, v34, v35, v36, v37, v38, v39, v40, v41, v42, v43, v44, v45, v46, v47, v48, v49, v50, v51, " +
        "v52, v53, v54, v55,v56, v57, v58, v59) - Returns aclineend_id, create_date, id, datasource_id, meas_type, max_value, " +
        "min_value, max_value_time, min_value_time, average, integral_power calculated from input columns",
    extended = "Example: \n"
        + "SELECT _FUNC_(aclineend_id, create_time, datasource_id, id, meas_type, v00, v01, v02, v03, v04, v05, v06, " +
        "v07, v08, v09, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20, v21, v22, v23, v24, v25, v26, v27, v28," +
        "v29, v30, v31, v32,v33, v34, v35, v36, v37, v38, v39, v40, v41, v42, v43, v44, v45, v46, v47, v48, v49, v50, " +
        "v51, v52, v53, v54, v55,v56, v57, v58, v59) FROM src LIMIT 1; \n")
public class PeakValueUDF extends GenericUDTF {

  private Date createDate = null;
  private final List<String> measTypes = Arrays.asList("13032001", "81302001", "81322001");

  @Override
  public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
    // 入参个数是65个
    List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
    if (inputFields.size() != 65) {
      throw new UDFArgumentException("UDF takes 65 argument, actually get " + inputFields.size());
    }

    // 函数调用日期 （精确到天）
    createDate = Date.from(LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant());

    // ------------------------------------------------------------------------
    //  返回值列名
    // ------------------------------------------------------------------------
    List<String> columnNames = Arrays.asList("aclineend_id", "create_date", "id", "datasource_id", "meas_type", "max_value",
        "min_value", "max_value_time", "min_value_time", "average", "integral_power", "create_time");

    // ------------------------------------------------------------------------
    //  返回值列类型
    // ------------------------------------------------------------------------

    ArrayList<ObjectInspector> columnTypes = new ArrayList<>();
    // 0.aclineend_id
    columnTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    // 1.create_date
    columnTypes.add(PrimitiveObjectInspectorFactory.javaTimestampObjectInspector);
    // 2.id
    columnTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    // 3.datasource_id
    columnTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    // 4.meas_type
    columnTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    // 5.max_value
    columnTypes.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    // 6.min_value
    columnTypes.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    // 7.max_value_time
    columnTypes.add(PrimitiveObjectInspectorFactory.javaTimestampObjectInspector);
    // 8.min_value_time
    columnTypes.add(PrimitiveObjectInspectorFactory.javaTimestampObjectInspector);
    // 9.average
    columnTypes.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    // 10.integral_power
    columnTypes.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    // 11.create_time
    columnTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    return ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnTypes);
  }

  @Override
  public void process(Object[] args) throws HiveException {

    // ------------------------------------------------------------------------
    //  计算结果
    // ------------------------------------------------------------------------
    Object[] calcResults = new Object[12];

    // ------------------------------------------------------------------------
    //  元数据
    // ------------------------------------------------------------------------
    // aclineend_id
    calcResults[0] = args[0];
    // create_date (精确到秒)
    calcResults[1] = new Timestamp(createDate.getTime());
//    calcResults[1] = args[1];
    // id
    calcResults[2] = args[3];
    // datasource_id
    calcResults[3] = args[2];
    // meas_type
    calcResults[4] = args[4];
    // create_time
    calcResults[11] = args[1];

    // 计算极值
    calculatePeakValue(args, calcResults);

    forward(calcResults);
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
    String measType = args[5] == null ? null : args[5].toString();

    Object[] deviceValues = Arrays.copyOfRange(args, 5, 65);

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
            maxValueTime = new Timestamp(createTime.getTime() + i * 60000L);
          }
        } else if (Objects.nonNull(doubleI) && max.compareTo(doubleI) < 0) {
          max = doubleI;
          maxValueTime = new Timestamp(createTime.getTime() + i * 60000L);
        }

        // 求最小值
        if (Objects.isNull(min)) {
          min = doubleI;
          // 最小值时间
          if (Objects.nonNull(min)) {
            minValueTime = new Timestamp(createTime.getTime() + i * 60000L);
          }
        } else if (Objects.nonNull(doubleI) && min.compareTo(doubleI) > 0) {
          min = doubleI;
          minValueTime = new Timestamp(createTime.getTime() + i * 60000L);
        }
        // 取绝对值的情况
      } else {
        // 求最大值
        if (Objects.isNull(max)) {
          max = doubleI;
          // 最大值时间
          if (Objects.nonNull(max)) {
            maxValueTime = new Timestamp(createTime.getTime() + i * 60000L);
          }
        } else if (Objects.nonNull(doubleI) && Math.abs(max) < (Math.abs(doubleI))) {
          max = doubleI;
          maxValueTime = new Timestamp(createTime.getTime() + i * 60000L);
        }

        // 求最小值
        if (Objects.isNull(min)) {
          min = doubleI;
          // 最小值时间
          if (Objects.nonNull(min)) {
            minValueTime = new Timestamp(createTime.getTime() + i * 60000L);
          }
        } else if (Objects.nonNull(doubleI) && Math.abs(min) > (Math.abs(doubleI))) {
          min = doubleI;
          minValueTime = new Timestamp(createTime.getTime() + i * 60000L);
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

    calcResults[5] = max;
    calcResults[6] = min;
    calcResults[7] = maxValueTime;
    calcResults[8] = minValueTime;
    calcResults[9] = average;
    calcResults[10] = integralPower;
  }

  @Override
  public void close() throws HiveException {
  }
}
