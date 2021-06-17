package tech.jiafan.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
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

    private String deal_other(Object other){
        if (other == null){
            return null;
        }else {
            return other.toString();
        }
    }

    public String evaluate(Object col, Object other) {
        long[] mask = { 0x000000FF, 0x0000FF00, 0x00FF0000, 0xFF000000 };

        try{
            long ipLong = Long.parseLong(col.toString());

            if (ipLong>4294967295L || ipLong<0){
                logger.warn(String.format("数字超出范围，无法转化 %d", ipLong));
                return deal_other(other);
            }
            long flag;
            StringBuilder ipInfo = new StringBuilder();

            for (int i = 0; i < 4; i++) {
                flag = (ipLong & mask[i]) >> (i * 8);
                if (i > 0)
                    ipInfo.insert(0, ".");
                ipInfo.insert(0, Long.toString(flag, 10));
            }
            return ipInfo.toString();
        }
        catch (Exception ex){
            logger.error("待转换值不是数字");
            logger.error(col);
            ex.printStackTrace();
            return deal_other(other);
        }
    }

    public String evaluate(Object col) {
        return evaluate(col, null);
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        
        return null;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        return null;
    }

    @Override
    public String getDisplayString(String[] children) {
        return null;
    }
}
