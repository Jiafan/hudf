package tech.jiafan.udf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * @Author 加帆
 * @Date 2021/6/05 06:02
 * @Version 1.0
 * @Description 值映射，类似Oracle中decode
 */

public class MDecode extends GenericUDF {
    private final Logger logger = LogManager.getLogger(MDecode.class);
    // 判断字符串是否可转化为数字
    private static boolean isNumeric(String s) {
        if (s != null && !"".equals(s.trim()))
            return s.matches("^[0-9]*$");
        else
            return false;
    }
    // 标准化数字：数字、数字字符串, ,消除如 1 、1.0、'1'、'1.0' 的差异
    private static String formatObject(Object s) {
        if ((s instanceof Integer) || ((s instanceof String) && isNumeric(s.toString()))){
            double num = Double.parseDouble(s.toString());
            return Double.toString(num);
        }else{
            // 其他类型
            return String.valueOf(s);
        }
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments==null || arguments.length %2 != 0){
            throw new UDFArgumentLengthException("mdecode()输入的参数个数错误，参数至少含4个，且个数应为偶数，参照 Oracle decode 函数");
        }
        else{
            for (int i=2; i<arguments.length-1; i+=2){
                if (!(arguments[i] instanceof StringObjectInspector)) {
                    throw new UDFArgumentTypeException(i, "目标值需为字符串类型");
                }
            }
            // 返回字符串类型
            return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
        }
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments.length<4 && arguments.length %2 != 0){
            logger.error("输入的参数个数错误，参数至少含4个，且个数应为偶数，参照 Oracle decode 函数");
            //非法参数， 返回 空
            return null;
        }
        Object result = null;
        DeferredObject target = arguments[0];
        String targetString = formatObject(target.get());

        for (int i=1; i<arguments.length-1; i+=2){
            String caseValue = formatObject(arguments[i].get());

            if (caseValue.equals(targetString)){
                result = arguments[i+1].get().toString();
                break;
            }
        }
        if (result == null){
            result = arguments[arguments.length-1].get().toString();
        }
        return new Text(String.valueOf(result));
    }

    @Override
    public String getDisplayString(String[] children) {
        return "mdecode() 映射解码函数，为简化 case 语句，可参照 oracle decode 函数使用";
    }
}
