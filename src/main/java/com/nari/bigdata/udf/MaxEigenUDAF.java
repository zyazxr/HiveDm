package com.nari.bigdata.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MaxEigenUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        // 验证第一个参数是否支持比较大小
        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
        if (!ObjectInspectorUtils.compareSupported(oi)) {
            throw new UDFArgumentTypeException(0, "Cannot support comparison of map<> type or complex type containing map<>.");
        }
        return new MaxEigenUDAFEvaluator();
    }

    public static class MaxEigenUDAFEvaluator extends GenericUDAFEvaluator {

        ObjectInspector[] inputOIs;
        ObjectInspector[] outputOIs;
        ObjectInspector   structOI;

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            super.init(mode, parameters);

            int length = parameters.length;
            if (length > 1 || !(parameters[0] instanceof StructObjectInspector)) {
                assert(mode == Mode.COMPLETE || mode == Mode.FINAL);
                initMapSide(parameters);

            } else {
                assert(mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2);
                assert(parameters.length == 1 && parameters[0] instanceof StructObjectInspector);
                initReduceSide((StructObjectInspector) parameters[0]);
            }

            return structOI;
        }

        //Initialize the UDAF on the map side.
        private void initMapSide(ObjectInspector[] parameters) throws HiveException {
            int length = parameters.length;
            outputOIs = new ObjectInspector[length];
            List<String> fieldNames = new ArrayList<String>(length);
            List<ObjectInspector> fieldOIs = Arrays.asList(outputOIs);

            for (int i = 0; i < length; i++) {
                fieldNames.add("col" + i);
                outputOIs[i] = ObjectInspectorUtils.getStandardObjectInspector(parameters[i]);
            }

            inputOIs = parameters;
            structOI = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
        }

        //Initialize the UDAF on the reduce side (or the map side in some cases)
        private void initReduceSide(StructObjectInspector inputStructOI) throws HiveException {
            List<? extends StructField> fields = inputStructOI.getAllStructFieldRefs();
            int length = fields.size();
            inputOIs = new ObjectInspector[length];
            outputOIs = new ObjectInspector[length];
            for (int i = 0; i < length; i++) {
                StructField field = fields.get(i);
                inputOIs[i]  = field.getFieldObjectInspector();
                outputOIs[i] = ObjectInspectorUtils.getStandardObjectInspector(inputOIs[i]);
            }
            structOI = ObjectInspectorUtils.getStandardObjectInspector(inputStructOI);
        }

        static class MaxAgg implements AggregationBuffer {
            Object[] objects;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MaxAgg result = new MaxAgg();
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            MaxAgg maxagg = (MaxAgg) agg;
            maxagg.objects = null;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            merge(agg, parameters);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                MaxAgg maxagg = (MaxAgg) agg;
                List<Object> objects;
                if (partial instanceof Object[]) {
                    objects = Arrays.asList((Object[]) partial);
                } else if (partial instanceof LazyBinaryStruct) {
                    objects = ((LazyBinaryStruct) partial).getFieldsAsList();
                } else {
                    throw new HiveException("Invalid type: " + partial.getClass().getName());
                }

                boolean isMax = false;
                if (maxagg.objects == null) {
                    isMax = true;
                } else {
                    int cmp = ObjectInspectorUtils.compare(maxagg.objects[0], outputOIs[0], objects.get(0), inputOIs[0]);
                    if (cmp < 0) {
                        isMax = true;
                    }
                }

                if (isMax) {
                    int length = objects.size();
                    maxagg.objects = new Object[length];
                    for (int i = 0; i < length; i++) {
                        maxagg.objects[i] = ObjectInspectorUtils.copyToStandardObject(objects.get(i), inputOIs[i]);
                    }
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MaxAgg maxagg = (MaxAgg) agg;
            return Arrays.asList(maxagg.objects);
        }
    }
}
