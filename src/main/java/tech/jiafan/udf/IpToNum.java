package tech.jiafan.udf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * @author 加帆
 * @version 1.0
 */
@Description(name = "ip_to_num", value = "_FUNC_(string) - ip_to_num 输入IPv4地址，转换为整数",
        extended = "输入一个合法的 IPv4 的地址"
                + "如果该列出现空值 或者 输入的字符串不是合法的 IPv4地址, 则返回空.\n"
                + "Example: > SELECT _FUNC_('123.123.22.4');\n Return: 2071664132")
public class IpToNum extends GenericUDF {
    private final Logger logger = LogManager.getLogger(IpToNum.class);
    private ObjectInspectorConverters.Converter[] converters;

    /** * 判断是否为合法IP * @return the ip */
    private boolean isIp(String ipAddress) {
        String ip = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";
        Pattern pattern = Pattern.compile(ip);
        Matcher matcher = pattern.matcher(ipAddress);
        return matcher.matches();
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments==null || arguments.length != 1){
            throw new UDFArgumentLengthException("参数长度异常：ip_to_num 接受一个string类型参数");
        }else if (!(arguments[0] instanceof StringObjectInspector)){
            throw new UDFArgumentTypeException(0, "ip_to_num 接受一个 string 类型参数");
        }else {
            converters = new ObjectInspectorConverters.Converter[arguments.length];
            for (int i = 0; i < arguments.length; i++) {
                converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
            }

            return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);
        }
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String srcIp = (String) converters[0].convert(arguments[0].get());
        if (srcIp == null) {
            return null;
        }else {
            if (isIp(srcIp)){
                String[] ip_pieces = srcIp.split("\\.");
                long a = Integer.parseInt(ip_pieces[0]);
                long b = Integer.parseInt(ip_pieces[1]);
                long c = Integer.parseInt(ip_pieces[2]);
                long d = Integer.parseInt(ip_pieces[3]);
                return new LongWritable(a * 256 * 256 * 256 + b * 256 * 256 + c * 256 + d);
            }else{
                logger.info(String.format("不是合法的IP地址 %s", srcIp));
                return null;
            }
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "ip_to_num() ip转数字";
    }
}
