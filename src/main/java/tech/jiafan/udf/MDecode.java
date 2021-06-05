package tech.jiafan.udf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author 加帆
 * @Date 2021/6/05 06:02
 * @Version 1.0
 * @Description 值映射，类似Oracle中decode
 */

public class MDecode extends UDF{
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

    public String evaluate(Object... args) {
        if (args.length<4 && args.length %2 != 0){
            logger.error("输入的参数个数错误，参数至少含4个，且个数应为偶数，参照 Oracle decode 函数");
            //非法参数， 返回 空
            return null;
        }
        Object result = null;
        args[0] = formatObject(args[0]);
        for (int i=1; i<args.length-1; i+=2){
            args[i] = formatObject(args[i]);

            if (String.valueOf(args[i]).equals(String.valueOf(args[0]))){
                result = args[i+1];
                break;
            }
        }
        if (result == null){
            result = args[args.length-1];
        }
        return String.valueOf(result);
    }

    public static void main(String[] args) {
        MDecode decode = new MDecode();
        System.out.println(decode.evaluate("3", 1, "个人", 2, "企业", 3, "特约", "其他"));
        System.out.println(decode.evaluate("5", 1, "个人", 2, "企业", 3, "特约",5,"子户", "其他"));
        System.out.println(decode.evaluate(3, '1', "个人", 2, "企业", "3", "特约", "其他"));
    }
}
