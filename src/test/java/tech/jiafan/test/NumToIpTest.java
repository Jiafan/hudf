package tech.jiafan.test;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;
import tech.jiafan.udf.NumToIp;

public class NumToIpTest {
    private  NumToIp numToIp;

    @Before
    public void testPrepare() throws UDFArgumentException {
        numToIp = new NumToIp();
        numToIp.initialize(new ObjectInspector[]{PrimitiveObjectInspectorFactory.
                getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG)});
    }

    @Test//测试正常的ip数值
    public void testNumToIpLegal() throws HiveException {
        Object ip = numToIp.evaluate(new GenericUDF.DeferredObject[]{new DeferredJavaObject(new Long(1242))});
        System.out.println(ip);
    }

    @Test//测试超过合法ip范围的整数
    public void testNumToIpIllegal() throws HiveException {
        Object ip = numToIp.evaluate(new GenericUDF.DeferredObject[]{new DeferredJavaObject(new Long(293213234001223L))});
        System.out.println(ip);
    }
}
