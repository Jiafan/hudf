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
import tech.jiafan.udf.IpToNum;

public class IpToNumTest {
    private IpToNum ipToNum;

    @Before
    public void testPrepare() throws UDFArgumentException {
        ipToNum = new IpToNum();
        ipToNum.initialize(new ObjectInspector[]{PrimitiveObjectInspectorFactory.
                getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING)});
    }

    @Test//测试合法ip
    public void testNumToIpLegal() throws HiveException {
        Object ip = ipToNum.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject("123.123.22.4")});
        System.out.println(ip);
        Assert.assertNotNull("合法IP 转换不应该空",ip);
    }

    @Test//测试非法ip
    public void testNumToIpIllegal() throws HiveException {
        Object ip = ipToNum.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject("123.123.232.1.2")});
        Assert.assertNull("非法IP 转换应该得空",ip);
    }
}

