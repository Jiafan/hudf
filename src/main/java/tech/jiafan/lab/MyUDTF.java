package tech.jiafan.lab;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

public class MyUDTF extends GenericUDTF {
    @Override
    public void process(Object[] args) throws HiveException {
        Date start = args[0].
    }

    @Override
    public void close() throws HiveException {

    }
}
