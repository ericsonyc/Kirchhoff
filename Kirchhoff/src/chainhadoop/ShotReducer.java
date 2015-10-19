package chainhadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by ericson on 2015/1/25 0025.
 */
public class ShotReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     *
     * @param key
     * @param values
     * @param context
     */
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println("reduceKey:" + key.get());
        Iterator<Text> vv = values.iterator();
        String str = "";
        while (vv.hasNext()) {
            str += vv.next().toString() + ",";
        }
        str = str.substring(0, str.lastIndexOf(","));
        System.out.println("str:" + str);
        context.write(key, new Text(str));
        System.out.println("reduceKey over");
    }

}
