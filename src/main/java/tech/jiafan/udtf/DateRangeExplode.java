package tech.jiafan.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import tech.jiafan.util.Util;

public class DateRangeExplode extends GenericUDTF {

    //输出数据的集合
    private ArrayList<DateWritable> outPutList = new ArrayList<>();

    private transient ObjectInspectorConverters.Converter inputConverter1;
    private transient ObjectInspectorConverters.Converter inputConverter2;
    private transient PrimitiveObjectInspector.PrimitiveCategory inputType1;
    private transient PrimitiveObjectInspector.PrimitiveCategory inputType2;
    private static Logger logger;
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        logger = LogManager.getLogger(StructObjectInspector.class);
        List<? extends StructField> allStructFieldRefs = argOIs.getAllStructFieldRefs();
        // 检查参数长度
        if (allStructFieldRefs.size() != 2) {
            throw new UDFArgumentException("DateRangeExplode() 需要两个参数");
        }
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaDateObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }
    @Override
    public void process(Object[] args) throws HiveException {
        try{
            Date start = Util.dateConvert(inputType1, inputConverter1,args[0]);
            Date end = Util.dateConvert(inputType2, inputConverter2,args[1]);
            if (start.getTime()>end.getTime()){
                throw new HiveException("DateRangeExplode() 起始时间必须早于截止时间");
            }

            outPutList.clear();
            Calendar calendar = Calendar.getInstance();
            while (start.getTime()<=end.getTime()){
                // 把日期添加到集合
                outPutList.add(new DateWritable(start));
                // 设置日期
                calendar.setTime(start);
                //把日期增加一天
                calendar.add(Calendar.DATE, 1);
                // 获取增加后的日期
                start=new Date(calendar.getTime().getTime());
            }
        } catch (UDFArgumentException udfAE){
            logger.error("该类型无法转化为时间类型：%s", udfAE);
        } catch (Exception commonE) {
            logger.error("遇到异常：%s", commonE);
        } finally {

        }
    }

    @Override
    public void close() throws HiveException {

    }
}
