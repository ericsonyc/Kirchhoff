package hdfswrite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CustomRecordWriter extends RecordWriter<Text, Text> {

    private List<Integer> aps = null;
    private List<String> image = null;
    private String filename = null;

    public CustomRecordWriter(String filename) {
        // TODO Auto-generated constructor stub
        System.out.println("CustomRecordWriter");
        aps = new ArrayList<Integer>();
        image = new ArrayList<String>();
        this.filename = filename;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        System.out.println("customwrite close");
        List<Integer> temp = new ArrayList<Integer>();
        for (int i = 0; i < aps.size(); i++) {
            temp.add(i);
        }
        this.quick_sort(aps, 0, aps.size() - 1, temp);
        if (aps.size() > 0)
            this.filename += "#" + aps.get(0);
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream fsOut = fs.create(new Path(filename), true);
        for (int i = 0; i < temp.size(); i++) {
            String[] strs = image.get(temp.get(i)).split(",");
            byte[] bytes = new byte[strs.length * 4];
            for (int j = 0; j < strs.length; j++) {
                this.getBytes(bytes, Float.floatToIntBits(Float.parseFloat(strs[j])), 4 * j);
            }
            fsOut.write(bytes, 0, bytes.length);
        }
        fsOut.close();
        System.gc();
    }

    @Override
    public void write(Text key, Text value) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        System.out.println("custom write");
        aps.add(Integer.parseInt(key.toString()));
        image.add(value.toString());
    }

    private void quick_sort(List<Integer> s, int l, int r, List<Integer> value) {
        if (l < r) {
            Swap(s, l, l + (r - l + 1) / 2);
            Swap(value, l, l + (r - l + 1) / 2);
            int i = l, j = r, x = s.get(l);
            int y = value.get(l);
            while (i < j) {
                while (i < j && s.get(j) >= x) {
                    j--;
                }
                if (i < j) {
                    s.set(i, s.get(j));
                    value.set(i, value.get(j));
                    i++;
                }
                while (i < j && s.get(i) < x) {
                    i++;
                }
                if (i < j) {
                    s.set(j, s.get(i));
                    value.set(j, value.get(i));
                    j--;
                }
            }
            s.set(i, x);
            value.set(i, y);
            quick_sort(s, l, i - 1, value);
            quick_sort(s, i + 1, r, value);
        }
    }

    private void Swap(List<Integer> s, int oldindex, int newindex) {
        Integer temp = s.get(oldindex);
        s.set(oldindex, s.get(newindex));
        s.set(newindex, temp);
    }

    public void getBytes(byte[] bytes, int data, int offset) {
        bytes[0 + offset] = (byte) (data & 0xff);
        bytes[1 + offset] = (byte) ((data & 0xff00) >> 8);
        bytes[2 + offset] = (byte) ((data & 0xff0000) >> 16);
        bytes[3 + offset] = (byte) ((data & 0xff000000) >> 24);
    }

//    private void getSort(ArrayList<Integer> aps, ArrayList<String> image) {
//        for (int i = 0; i < aps.size(); i++) {
//            for (int j = 0; j < aps.size() - i - 1; j++) {
//                if (aps.get(j) > aps.get(j + 1)) {
//                    int temp = aps.get(j);
//                    aps.set(j, aps.get(j + 1));
//                    aps.set(j + 1, temp);
//                    String ff = image.get(j);
//                    image.set(j, image.get(j + 1));
//                    image.set(j + 1, ff);
//                }
//            }
//        }
//    }
}
