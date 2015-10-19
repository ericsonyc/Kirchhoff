package chainhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ericson on 2015/1/26 0026.
 */
public class WriteMap extends Mapper<IntWritable, Text, Text, Text> {

    private List<Integer> keyLists = new ArrayList<Integer>();
    private List<String> offsetLists = new ArrayList<String>();

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     *
     * @param key
     * @param value
     * @param context
     */
    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("map_key:" + key.get());
        Path output = FileOutputFormat.getOutputPath(context);
        String filename = "";
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
//        byte[] bytes = new byte[4];
//        this.getBytes(key.get(), bytes, 0);
        String taskId = context.getTaskAttemptID().toString();
        taskId = taskId.substring(taskId.lastIndexOf("r")) + "@" + key.get();
        System.out.println("map taskId:" + taskId);
        FSDataOutputStream fsOut = fs.create(new Path(output.toString() + "/" + taskId));
//        fsOut.write(bytes);
        FSDataInputStream fsIn = null;
        byte[] temp = null;
        for (int i = 0; i < keyLists.size(); i++) {
            int k = keyLists.get(i);
            String[] v = offsetLists.get(i).split("#");
            if (v[0] != filename) {
                if (fsIn != null)
                    fsIn.close();
                fsIn = fs.open(new Path(v[0]));
                temp = new byte[Integer.parseInt(v[2])];
            }
            fsIn.read(Long.parseLong(v[1]), temp, 0, Integer.parseInt(v[2]));
            fsOut.write(temp, 0, Integer.parseInt(v[2]));
        }
        fsIn.close();
        fsOut.close();
//        context.write(new Text("key:" + keyLists.get(0)), new Text("value:" + offsetLists.get(0)));
    }

    /**
     * Called once at the end of the task.
     *
     * @param context
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        keyLists = null;
        offsetLists = null;
        System.gc();
    }

    /**
     * Expert users can override this method for more complete control over the
     * execution of the Mapper.
     *
     * @param context
     * @throws java.io.IOException
     */
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        System.out.println("writemap run");
        setup(context);
        try {
            System.out.println("run keylist.length:" + keyLists.size());
            map(new IntWritable(keyLists.get(0)), new Text(offsetLists.get(0)), context);
        } finally {
            cleanup(context);
        }
    }

    /**
     * Called once at the beginning of the task.
     *
     * @param context
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("setup");
        int count = 0;
        while (context.nextKeyValue()) {
            keyLists.add(count++, context.getCurrentKey().get());
            System.out.println("currentKey:" + keyLists.get(count - 1));
            offsetLists.add(context.getCurrentValue().toString());
        }
        System.out.println("count:" + count);
        this.quick_sort(keyLists, 0, keyLists.size() - 1, offsetLists);
        System.out.println("keyLists.size:" + keyLists.size());
    }

    private void quick_sort(List<Integer> s, int l, int r, List<String> offsetLists) {
        if (l < r) {
            Swap(s, l, l + (r - l + 1) / 2);
            Swap(offsetLists, l, l + (r - l + 1) / 2);
            int i = l, j = r, x = s.get(l);
            while (i < j) {
                while (i < j && s.get(j) >= x) {
                    j--;
                }
                if (i < j)
                    s.set(i++, s.get(j));
                while (i < j && s.get(i) < x) {
                    i++;
                }
                if (i < j)
                    s.set(j--, s.get(i));
            }
            s.set(i, x);
            quick_sort(s, l, i - 1, offsetLists);
            quick_sort(s, i + 1, r, offsetLists);
        }
    }

    private <T> void Swap(List<T> s, int oldindex, int newindex) {
        T temp = s.get(oldindex);
        s.set(oldindex, s.get(newindex));
        s.set(newindex, temp);
    }

    public void getBytes(int data, byte[] bytes, int offset) {
        bytes[offset] = (byte) (data & 0xff);
        bytes[1 + offset] = (byte) ((data & 0xff00) >> 8);
        bytes[2 + offset] = (byte) ((data & 0xff0000) >> 16);
        bytes[3 + offset] = (byte) ((data & 0xff000000) >> 24);
    }
}
