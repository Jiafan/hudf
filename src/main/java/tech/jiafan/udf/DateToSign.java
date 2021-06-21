package tech.jiafan.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import java.util.Calendar;
import java.util.Date;
import tech.jiafan.util.DateConverter;

/**
 * @author 加帆
 * @version 1.0
 */
//@Description(name = "date_to_sign", value = "_FUNC_(input date/timestamp/string) - date_to_sign input date convert to Zodiac Signs",
//        extended = "input can be date、timestamp or string"
//                + "If either argument is NULL or the value can not convert to a legal date, the return value is NULL.\n"
//                + "Example: > SELECT _FUNC_('2021-06-21');\n 'Gemini'")
@Description(name = "date_to_sign", value = "_FUNC_(input date/timestamp/string) - date_to_sign 输入时间字段转换为12星座",
        extended = "输入时间可为 date、timestamp 或者 指定格式字符串（yyyy-MM-dd）"
                + "如果该列出现空值 或者 输入的字符串不是 'yyyy-MM-dd' 格式无法转换为时间, 则返回空.\n"
                + "Example: > SELECT _FUNC_('2021-06-21');\n '双子座'")
public class DateToSign extends GenericUDF {
    private final Logger logger = LogManager.getLogger(DateToSign.class);
    private DateConverter converter;

    private String mapToSign(int month, int day){
        int int_day = month*100+day;
        if (int_day>=120 && int_day<=218){
            return "水瓶座";//Aquarius
        }
        else if (int_day>=219 && int_day<=320){
            return "双鱼座";//Pisces
        }
        else if (int_day>=321 && int_day<=420){
            return "白羊座";//Aries
        }
        else if (int_day>=421 && int_day<=520){
            return "金牛座";//Taurus
        }
        else if (int_day>=521 && int_day<=621){
            return "双子座";//Gemini
        }
        else if (int_day>=622 && int_day<=722){
            return "巨蟹座";//Cancer
        }
        else if (int_day>=723 && int_day<=822){
            return "狮子座";//Leo
        }
        else if (int_day>=823 && int_day<=922){
            return "处女座";//Virgo
        }
        else if (int_day>=923 && int_day<=1022){
            return "天秤座";//Libra
        }
        else if (int_day>=1023 && int_day<=1121){
            return "天蝎座";//Scorpio
        }
        else if (int_day>=1123 && int_day<=1221){
            return "射手座";//Sagittarius
        }
        else{
            //1222-0119
            return "摩羯座";//Capricorn
        }
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments==null || arguments.length!=1){
            throw new UDFArgumentLengthException("date_to_sign() 需要一个参数");
        }
        converter = new DateConverter(arguments[0]);
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments==null || arguments.length!=1){
            logger.info("实际参数异常");
            return null;
        }else {
            Date birthday = converter.convert(arguments[0].get());
            if (birthday!=null){
                Calendar calender = Calendar.getInstance();
                calender.setTime(birthday);
                int month=calender.get(Calendar.MONTH)+1;//获取月份
                int day=calender.get(Calendar.DATE);//获取日
                return new Text(mapToSign(month, day));
            }
            else {
                logger.info("转化为date异常");
                return null;
            }
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("date_to_sign() 日期生成星座", children);
    }
}
