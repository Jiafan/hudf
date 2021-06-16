package tech.jiafan.util;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


public class DateConverter {
    private transient Converter converter;
    private transient PrimitiveObjectInspector.PrimitiveCategory inputType;
    private transient final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    public DateConverter(ObjectInspector objectInspector){
        inputType = ((PrimitiveObjectInspector) objectInspector).getPrimitiveCategory();
        switch (inputType) {
            case STRING:
            case VARCHAR:
            case CHAR:
                converter = ObjectInspectorConverters.getConverter(objectInspector, PrimitiveObjectInspectorFactory.writableStringObjectInspector);
            case TIMESTAMP:
                converter =  new PrimitiveObjectInspectorConverter.TimestampConverter((PrimitiveObjectInspector) objectInspector,
                        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
            case DATE:
                converter =  ObjectInspectorConverters.getConverter(objectInspector,
                        PrimitiveObjectInspectorFactory.writableDateObjectInspector);
            default:
                converter = null;
        }
    }

    public Date convert(Object src) throws HiveException {
        Date date = new Date(0);
        switch (inputType) {
            case STRING:
            case VARCHAR:
            case CHAR:
                String dateString = converter.convert(src).toString();
                try {
                    date.setTime(formatter.parse(dateString).getTime());
                } catch (IllegalArgumentException | ParseException e) {
                    return null;
                }
                break;
            case TIMESTAMP:
                Timestamp ts = ((TimestampWritable) converter.convert(src))
                        .getTimestamp();
                date.setTime(ts.getTime());
                break;
            case DATE:
                DateWritable dw = (DateWritable) converter.convert(src);
                date = dw.get();
                break;
            default:
                throw new UDFArgumentException("DateRangeExplode() only takes STRING/TIMESTAMP/DATEWRITABLE types, got " + inputType);
        }
        return date;
    }

    public static Boolean evaluate(ObjectInspector objectInspector) throws UDFArgumentException {
        if (objectInspector.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + objectInspector.getTypeName() + " is passed. as first arguments");
        }
        final PrimitiveObjectInspector.PrimitiveCategory inputType =
                ((PrimitiveObjectInspector) objectInspector).getPrimitiveCategory();
        switch (inputType){
            case STRING:
            case VARCHAR:
            case CHAR:
            case TIMESTAMP:
            case DATE:
                return Boolean.TRUE;
            default:
                return false;
        }
    }
}
