package tech.jiafan.util;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Util {
    static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    public static Date dateConvert(PrimitiveObjectInspector.PrimitiveCategory inputType, ObjectInspectorConverters.Converter converter, Object argument) throws UDFArgumentException {
        if (argument == null) {
            return null;
        }
        Date date = new Date(0);
        switch (inputType) {
            case STRING:
            case VARCHAR:
            case CHAR:
                String dateString = converter.convert(argument).toString();
                try {
                    date.setTime(formatter.parse(dateString).getTime());
                } catch (IllegalArgumentException | ParseException e) {
                    return null;
                }
                break;
            case TIMESTAMP:
                Timestamp ts = ((TimestampWritable) converter.convert(argument))
                        .getTimestamp();
                date.setTime(ts.getTime());
                break;
            case DATE:
                DateWritable dw = (DateWritable) converter.convert(argument);
                date = dw.get();
                break;
            default:
                throw new UDFArgumentException("该类型无法转化为时间类型，可转换类型有： STRING/TIMESTAMP/DATEWRITABLE, 实际类型为 " + inputType);
        }
        return date;
    }
}
