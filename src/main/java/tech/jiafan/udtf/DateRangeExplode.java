package tech.jiafan.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;

import java.sql.Timestamp;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;


public class DateRangeExplode extends GenericUDTF {
    private transient SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    private transient ObjectInspectorConverters.Converter inputConverter1;
    private transient ObjectInspectorConverters.Converter inputConverter2;
    private transient PrimitiveObjectInspector.PrimitiveCategory inputType1;
    private transient PrimitiveObjectInspector.PrimitiveCategory inputType2;
    private static Logger logger;
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        logger = LogManager.getLogger(StructObjectInspector.class);
        if (argOIs.length != 2) {
            throw new UDFArgumentException("DateRangeExplode() 需要两个参数");
        }
        inputConverter1 = checkArguments(argOIs, 0);
        inputConverter2 = checkArguments(argOIs, 1);
        inputType1 = ((PrimitiveObjectInspector) argOIs[0]).getPrimitiveCategory();
        inputType2 = ((PrimitiveObjectInspector) argOIs[1]).getPrimitiveCategory();
    }
    @Override
    public void process(Object[] args) throws HiveException {
        Date start = convertToDate(inputType1, inputConverter1,args[0]);
        Date end = convertToDate(inputType2, inputConverter2,args[1]);
        if (start.getTime()>end.getTime()){
            throw new HiveException("DateRangeExplode() 起始时间必须早于截止时间");
        }

        Calendar c = Calendar.getInstance();
        Object[] res = new Object[1];
        res[0] = new DateWritable(start);
        do{
            forward(res);
            c.setTime(start);
            c.add(Calendar.DAY_OF_MONTH, 1);
            start = new Date(c.getTime().getTime());
            res[0] = new DateWritable(start);
        }while (start.getTime() <= end.getTime());
    }

    @Override
    public void close() throws HiveException {

    }
    private Date convertToDate(PrimitiveObjectInspector.PrimitiveCategory inputType, Converter converter, Object argument) throws HiveException{
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
                throw new UDFArgumentException("DateRangeExplode() only takes STRING/TIMESTAMP/DATEWRITABLE types, got " + inputType);
        }
        return date;
    }
    private Converter checkArguments(ObjectInspector[] arguments, int i) throws UDFArgumentException {
        if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + arguments[i].getTypeName() + " is passed. as first arguments");
        }
        final PrimitiveObjectInspector.PrimitiveCategory inputType =
                ((PrimitiveObjectInspector) arguments[i]).getPrimitiveCategory();
        switch (inputType) {
            case STRING:
            case VARCHAR:
            case CHAR:
                return ObjectInspectorConverters.getConverter(arguments[i],
                        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
            case TIMESTAMP:
                return new PrimitiveObjectInspectorConverter.TimestampConverter((PrimitiveObjectInspector) arguments[i],
                        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
            case DATE:
                return ObjectInspectorConverters.getConverter(arguments[i],
                        PrimitiveObjectInspectorFactory.writableDateObjectInspector);
            default:
                throw new UDFArgumentException(
                        " DateRangeExplode() only takes STRING/TIMESTAMP/DATEWRITABLE types as " + (i + 1)
                                + "-th argument, got " + inputType);
        }
    }
}
