package tech.jiafan.udf;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import java.util.Calendar;
import java.util.Date;


/**
 * @Author 加帆
 * @Date 2021/6/05 11:12
 * @Version 1.0
 * @Description 获取日期对应星座
 */
public class DateToSign {
    private final Logger logger = LogManager.getLogger(DateToSign.class);

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

    public String evaluate(Date birthday) {
        if (birthday!=null){
            Calendar calender = Calendar.getInstance();
            calender.setTime(birthday);
            int month=calender.get(Calendar.MONTH)+1;//获取月份
            int day=calender.get(Calendar.DATE);//获取日
            return mapToSign(month, day);
        }
        else {
            logger.info("参数日期为空");
            return null;
        }
    }
}
