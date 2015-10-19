package chainhadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class Mapper1 extends Mapper<IntWritable, Text, IntWritable, Text> {

    @Override
    public void map(IntWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] datas = value.toString().split(",");
        int[] keys = new int[datas.length / 2];
        float[] fcxydatas = new float[datas.length];
        for (int i = 0; i < datas.length / 2; i++) {
            keys[i] = key.get() + i;
            fcxydatas[2 * i] = Float.parseFloat(datas[2 * i]);
            fcxydatas[2 * i + 1] = Float.parseFloat(datas[2 * i + 1]);
        }
        datas = null;
        try {
            System.out.println("key:" + keys[0] + ",value:" + fcxydatas[0] + "," + fcxydatas[1]);
            System.out.println("begin map");
            long start = System.currentTimeMillis();

            HashMap<Integer, String> maplists = new HashMap<Integer, String>();
            GetOutputShot outputShot = new GetOutputShot(keys, fcxydatas, context.getConfiguration(), maplists);
            outputShot.runOutputShot();

            long end = System.currentTimeMillis();
            System.out.println("OutputTime:" + (end - start) + "ms");

            for (Entry<Integer, String> map : maplists.entrySet()) {
                context.write(new IntWritable(map.getKey()), new Text(map.getValue()));
            }

            long end1 = System.currentTimeMillis();
            System.out.println("mapTime:" + (end1 - end) + "ms");
        } catch (Exception e) {
            System.out.println("Exception in the map fuction");
            e.printStackTrace();
        }
    }

    /**
     * Called once at the end of the task.
     *
     * @param context
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.gc();
    }
}
