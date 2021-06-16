package tech.jiafan.test;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;
import tech.jiafan.util.DateConverter;

import java.sql.Date;

public class DateConverterTest {
    @Test
    public void testEvaluateDate() throws UDFArgumentException {
        //Int
        ObjectInspector intInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.DATE);
        Assert.assertEquals("不可转换", Boolean.TRUE, DateConverter.evaluate(intInspector));
    }
    @Test
    public void testEvaluateFloat() throws UDFArgumentException {
        //Int
        ObjectInspector intInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.FLOAT);
        Assert.assertEquals("可转换", Boolean.TRUE, DateConverter.evaluate(intInspector));
    }
    @Test
    public void testConvertDate() throws HiveException {
        ObjectInspector dateInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.DATE);

        //Date
        DateConverter dateConverter= new DateConverter(dateInspector);
        Date srcDate = dateConverter.convert(new Date(System.currentTimeMillis()));
        Assert.assertNotNull(srcDate);
    }
}
