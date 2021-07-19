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
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;


/**
 * @Author 加帆
 * @Date 2021/6/16 22:48
 * @Version 1.0
 * @Description 验证链接地址可访问性
 */
public class URLVerify extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments==null || arguments.length != 1){
            throw new UDFArgumentLengthException("参数长度异常：url_verify 接受一个string类型参数");
        }else if (!(arguments[0] instanceof StringObjectInspector) && !(arguments[0] instanceof LongObjectInspector) && !(arguments[0] instanceof IntObjectInspector)){
            throw new UDFArgumentTypeException(0, "url_verify 接受一个 string 类型参数");
        }else {
            HttpURLConnection.setFollowRedirects(false);
            return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);
        }
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments==null || arguments.length != 1){
            return null;
        }else {
            String url = arguments[0].get().toString();
            // 1. 先进行正则合法性检测
            if (is_url(url)) {
                // 2. 通过 http/https 访问
                return new IntWritable(is_accessible(url));
            }else{
                return new IntWritable(-1);// -1 ,url 格式非法
            }
        }

    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("urlVerify() 做url验证函数，验证 url 的可访问性", children);
    }

    private  int is_accessible(String url){
        // 2. 尝试访问获取访问http 状态码
        try {
            HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
            //从 HTTP 响应消息获取状态码
            return (con.getResponseCode() == HttpURLConnection.HTTP_OK)?1:0;
        }catch (MalformedURLException e) {
            e.printStackTrace();
            return 0;
        }catch (IOException e) {
            return 0;
        }
    }

    private boolean is_url(String str){
        str = str.toLowerCase();
        String regex = "^((https|http)?://)"  //https、http
                + "?(([0-9a-z_!~*'().&=+$%-]+: )?[0-9a-z_!~*'().&=+$%-]+@)?" //ftp的user@
                + "(([0-9]{1,3}\\.){3}[0-9]{1,3}" // IP形式的URL- 例如：199.194.52.184
                + "|" // 允许IP和DOMAIN（域名）
                + "([0-9a-z_!~*'()-]+\\.)*" // 域名- www.
                + "([0-9a-z][0-9a-z-]{0,61})?[0-9a-z]\\." // 二级域名
                + "[a-z]{2,6})" // first level domain- .com or .museum
                + "(:[0-9]{1,5})?" // 端口号最大为65535,5位数
                + "((/?)|" // a slash isn't required if there is no file name
                + "(/[0-9a-z_!~*'().;?:@&=+$,%#-]+)+/?)$";
        return  str.matches(regex);
    }
}
