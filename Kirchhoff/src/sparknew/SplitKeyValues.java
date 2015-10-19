package sparknew;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ericson on 2015/4/5 0005.
 */
public class SplitKeyValues implements PairFlatMapFunction<Tuple2<Integer, String>, Integer, String> {
    @Override
    public Iterable<Tuple2<Integer, String>> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
        List<Tuple2<Integer, String>> array = new ArrayList<Tuple2<Integer, String>>();
        String[] datas = integerStringTuple2._2().split(",");
        int[] keys = new int[datas.length / 2];
        float[] fcxydatas = new float[datas.length];
        for (int i = 0; i < datas.length / 2; i++) {
            keys[i] = integerStringTuple2._1().intValue() + i;
            fcxydatas[2 * i] = Float.parseFloat(datas[2 * i]);
            fcxydatas[2 * i + 1] = Float.parseFloat(datas[2 * i + 1]);
            array.add(new Tuple2<Integer, String>(keys[i], fcxydatas[2 * i] + "," + fcxydatas[2 * i + 1]));
        }
        return array;
    }
}
