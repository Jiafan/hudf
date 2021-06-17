package tech.jiafan.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * @Author 加帆
 * @Date 2021/6/05 10:48
 * @Version 1.0
 * @Description 整数还原为 IPv4
 */
public class NumToIp extends GenericUDF {
    private final Logger logger = LogManager.getLogger(NumToIp.class);
    private transient PrimitiveObjectInspector.PrimitiveCategory inputType;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments==null || arguments.length != 1){
            throw new UDFArgumentLengthException("参数长度异常：ip_to_num 接受一个string类型参数");
        }else if (!(arguments[0] instanceof StringObjectInspector) && !(arguments[0] instanceof LongObjectInspector) && !(arguments[0] instanceof IntObjectInspector)){
            throw new UDFArgumentTypeException(0, "num_to_ip 接受一个 string/int/long 类型参数");
        }else {
            inputType = ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory();
            return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
        }
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if(arguments==null || arguments.length != 1 || arguments[0]==null){
            return null;
        }else{
            long[] mask = { 0x000000FF, 0x0000FF00, 0x00FF0000, 0xFF000000 };
            long ipLong = inputType== PrimitiveObjectInspector.PrimitiveCategory.STRING?Long.parseLong(arguments[0].get().toString()):Long.valueOf(arguments[0].get().toString());
            if (ipLong>4294967295L || ipLong<0){
                logger.warn(String.format("数字超出范围，无法转化 %d", ipLong));
                return null;
            }
            long flag;
            StringBuilder ipInfo = new StringBuilder();

            for (int i = 0; i < 4; i++) {
                flag = (ipLong & mask[i]) >> (i * 8);
                if (i > 0)
                    ipInfo.insert(0, ".");
                ipInfo.insert(0, Long.toString(flag, 10));
            }
            return new Text(ipInfo.toString());
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return null;
    }
}
