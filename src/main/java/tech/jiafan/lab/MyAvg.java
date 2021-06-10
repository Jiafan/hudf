package tech.jiafan.lab;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class MyAvg extends UDAF{
    public static class AvgState {
        private long mCount;
        private double mSum;
    }

    public static class MyAvgEvaluator implements UDAFEvaluator {
        AvgState state;

        public MyAvgEvaluator(){
            super();
            state = new AvgState();
            init();
        }

        @Override
        public void init() {
            state.mSum=0;
            state.mCount=0;
        }

        public boolean iterate(Double in){
            if (in != null){
                state.mSum += in;
                state.mCount += 1;
            }
            return true;
        }
        public AvgState terminatePartial(){
            return state.mCount == 0? null: state;
        }
        public boolean merge(AvgState part_state){
            if (part_state != null){
                state.mSum += part_state.mSum;
                part_state.mCount += part_state.mCount;
            }
            return true;
        }
        public Double terminate(){
            return state.mCount == 0? null: Double.valueOf(state.mSum/state.mCount);
        }
    }
}
