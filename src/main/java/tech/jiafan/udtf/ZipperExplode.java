package tech.jiafan.udtf;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class ZipperExplode extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs); // 创建一个Struct类型返回
    }

    @Override
    public void process(Object[] args) throws HiveException {

    }

    @Override
    public void close() throws HiveException {

    }
}
