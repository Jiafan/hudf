package tech.jiafan.udf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author 加帆
 * @Date 2021/6/05 11:12
 * @Version 1.0
 */
public class IpToNum extends UDF{
    private final Logger logger = LogManager.getLogger(IpToNum.class);

    /** * 判断是否为合法IP * @return the ip */
    private boolean isIp(String ipAddress) {
        String ip = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";
        Pattern pattern = Pattern.compile(ip);
        Matcher matcher = pattern.matcher(ipAddress);
        return matcher.matches();
    }

    public Long evaluate(Object ip, Object other) {
        String ipAddress = ip.toString();
        if (isIp(ipAddress)){
            String[] ip_pieces = ipAddress.split("\\.");
            long a = Integer.parseInt(ip_pieces[0]);
            long b = Integer.parseInt(ip_pieces[1]);
            long c = Integer.parseInt(ip_pieces[2]);
            long d = Integer.parseInt(ip_pieces[3]);
            return a * 256 * 256 * 256 + b * 256 * 256 + c * 256 + d;
        }
        else if (other == null){
            logger.warn(String.format("非法IP地址:%s", ipAddress));
            return null;
        }
        try {
            return Long.valueOf(other.toString());
        }
        catch (Exception ex){
            logger.warn("IP地址非法，且 默认值非法，不是长整数");
            ex.printStackTrace();
            return null;
        }
    }

    public Long evaluate(Object ip) {
        return evaluate(ip, null);
    }

    public static void main(String[] args) {
        IpToNum itn = new IpToNum();
        System.out.println(itn.evaluate("123.1.23.2"));
        System.out.println(itn.evaluate("324.1.23.2", 0L));
        System.out.println(itn.evaluate("255.255.255.255"));

    }

}
