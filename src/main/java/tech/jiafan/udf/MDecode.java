package tech.jiafan.udf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.hive.ql.exec.UDF;

public class MDecode extends UDF{
    private final Logger logger = LogManager.getLogger(MDecode.class);

    public String evaluate(Object... args) {
        if (args.length<4 && args.length %2 != 0){
            logger.error("输入的参数个数错误，参数至少含4个，且个数应为偶数，参照 Oracle decode 函数");
            return "DecodeException：args error";
        }

        Object result = null;

        if(args[0] instanceof Integer){
            args[0] = (double) Integer.parseInt(args[0].toString());
        }

        for (int i=1; i<args.length-1; i+=2){
            if (args[i] instanceof Integer){
                args[i] = (double) Integer.parseInt(args[i].toString());
            }
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
        System.out.println(decode.evaluate(5, 1, "个人", 2, "企业", 3, "特约", "其他"));
    }
}
