package tech.jiafan.test;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tech.jiafan.udf.MDecode;

public class MDecodeTest {
    private MDecode decoder;

    @Before
    public void testBefore() throws UDFArgumentException {
        decoder = new MDecode();
        ObjectInspector[] objectInspectors = new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING),
                PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING),
                PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING),
                PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING),
                PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING),
                PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING),
        };
        decoder.initialize(objectInspectors);
    }
    @Test
    public void decodeInt(){

    }
    @Test
    public void decodeString() throws HiveException {
        GenericUDF.DeferredObject[] deferredObjects = new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject("gaga"),
                new GenericUDF.DeferredJavaObject("App"),
                new GenericUDF.DeferredJavaObject("手机"),
                new GenericUDF.DeferredJavaObject("H5"),
                new GenericUDF.DeferredJavaObject("手机"),
                new GenericUDF.DeferredJavaObject("PC"),
                new GenericUDF.DeferredJavaObject("桌面"),
                new GenericUDF.DeferredJavaObject("接口")
        };
        String result = decoder.evaluate(deferredObjects).toString();
        System.out.println(result);
    }
}
