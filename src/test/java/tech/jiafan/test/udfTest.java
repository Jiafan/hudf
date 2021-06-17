package tech.jiafan.test;

import org.junit.Test;
import tech.jiafan.udf.DateToSign;
import tech.jiafan.udf.IpToNum;
import tech.jiafan.udf.MDecode;
import tech.jiafan.udf.NumToIp;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class udfTest {
    @Test
    public void  testIpToNum(){
        IpToNum itn = new IpToNum();
//        System.out.println(itn.evaluate("123.1.23.2"));
//        System.out.println(itn.evaluate("324.1.23.2", 0L));
//        System.out.println(itn.evaluate("255.255.255.255"));
    }

    @Test
    public void  testNumToIp(){
        NumToIp nti = new NumToIp();
        System.out.println(nti.evaluate(123,"-"));
        System.out.println(nti.evaluate("a123"));
        System.out.println(nti.evaluate(999999999999L));
    }

    @Test
    public void testDateToSign() throws ParseException {
        DateToSign dateToSign = new DateToSign();
        SimpleDateFormat formatter =  new SimpleDateFormat("yyyyMMdd");
//        System.out.println(dateToSign.evaluate(formatter.parse("20210101")));
//        System.out.println(dateToSign.evaluate(formatter.parse("20211205")));
//        System.out.println(dateToSign.evaluate(formatter.parse("20211222")));
//        System.out.println(dateToSign.evaluate(formatter.parse("20210605")));
    }

    @Test
    public void testMDecode(){
        MDecode decode = new MDecode();
        System.out.println(decode.evaluate("3", 1, "个人", 2, "企业", 3, "特约", "其他"));
        System.out.println(decode.evaluate("5", 1, "个人", 2, "企业", 3, "特约",5,"子户", "其他"));
        System.out.println(decode.evaluate(3, '1', "个人", 2, "企业", "3", "特约", "其他"));
    }
}
