package hdfswrite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ReadFileReducer extends Reducer<Text, Text, Text, Text> {


    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        long start = System.currentTimeMillis();
        Iterator<Text> ites = values.iterator();
        try {
//            System.out.println("a");
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            String path = conf.get("path");
//            System.out.println("pp:" + path);
            FSDataInputStream fsIn = null;
            int ont = conf.getInt("ont", 1);
            int length = 0;
            byte[] dataBytes = new byte[ont * Float.SIZE / 8];
            float[] vv = new float[ont];
            for (int i = 0; i < vv.length; i++) {
                vv[i] = 0;
            }
//            System.out.println("ddddddddd");
//            int count = 0;
            while (ites.hasNext()) {
//                count++;
//                System.out.println("next");
                String[] files = ites.next().toString().split("@");
                for (int i = 0; i < files.length; i++) {
//                    System.out.println(files[i]);
                    String[] temp = files[i].split("#");
                    fsIn = fs.open(new Path(path + "/map/" + temp[0]));
//                    System.out.println("path:" + temp[2]);
                    fsIn.seek(Long.parseLong(temp[1]));
                    length = Integer.parseInt(temp[2]);
//                    System.out.println("ont:" + ont + ",length:" + length);
                    fsIn.read(dataBytes, 0, length * Float.SIZE / 8);
                    fsIn.close();
                    for (int j = 0; j < length; j++) {
                        vv[j] += Float.intBitsToFloat(getInt(dataBytes, j * Float.SIZE / 8));
                    }
//                    System.out.println("woqualalala");
                }
            }
//            System.out.println("count:" + count);
//            System.out.println("get");
            String result = "";
            for (int i = 0; i < length; i++) {
                result += vv[i] + ",";
            }
            result = result.substring(0, result.lastIndexOf(","));
            context.write(key, new Text(result));
            long end = System.currentTimeMillis();
            System.out.println("reduce time:" + (end - start) + "ms");
        } catch (Exception e) {
            System.out.println("message:" + e.getMessage());
            e.printStackTrace();
        }

//        System.out.println("Reducer");
//        Iterator<Text> ites = values.iterator();
//        String connect = ites.next().toString();
//        String[] temps = connect.split(",");
//        float[] datas = new float[temps.length];
//        for (int i = 0; i < temps.length; i++) {
//            datas[i] = Float.parseFloat(temps[i]);
//        }
//
//        while (ites.hasNext()) {
//            connect = ites.next().toString();
//            temps = connect.split(",");
//            for (int i = 0; i < temps.length; i++) {
//                datas[i] += Float.parseFloat(temps[i]);
//            }
//        }
//        String result = "";
//        for (int i = 0; i < datas.length; i++) {
//            result += String.valueOf(datas[i]) + ",";
//        }
//        result = result.substring(0, result.lastIndexOf(","));
//        context.write(key, new Text(result));

        //System.out.println("Reducer key:" + key.toString());
//        Iterator<Text> ites = values.iterator();
//        try {
//            String path = context.getConfiguration().get("path");
//            FileSystem fs = FileSystem.get(context.getConfiguration());
//            int length = 0;
//            long offset = 0;
//            List<Float> lists = new ArrayList<Float>();
//            Path output = null;
//            while (ites.hasNext()) {
//                String[] message = ites.next().toString().split("#");
//                //System.out.println("ites:" + message[0] + "," + message[1] + "," + message[2]);
//                String[] filenames = message[0].split("$");
//                String[] offsets = message[1].split("$");
//                length = Integer.parseInt(message[2]);
//                byte[] buffer = new byte[length];
//                for (int i = 0; i < filenames.length; i++) {
//                    output = new Path(path + "/map/" + filenames[i]);
//                    FSDataInputStream fsIn = fs.open(output);
//                    offset = Long.parseLong(offsets[i]);
//                    fsIn.read(offset, buffer, 0, length);
//                    fsIn.close();
//                    if (lists.size() == 0) {
//                        for (int j = 0; j < length / 4; j++) {
//                            lists.add(Float.intBitsToFloat(getInt(buffer, 4 * j)));
//                        }
//                    } else {
//                        for (int j = 0; j < length / 4; j++) {
//                            lists.set(j, Float.intBitsToFloat(getInt(buffer, 4 * j)) + lists.get(j));
//                        }
//                    }
//                }
////                output = new Path(path + "map/" + message[0]);
////                FSDataInputStream fsIn = fs.open(output);
////                offset = Long.parseLong(message[1]);
////                length = Integer.parseInt(message[2]);
////                byte[] buffer = new byte[length];
////                fsIn.read(offset, buffer, 0, length);
////                fsIn.close();
////                if (lists.size() == 0) {
////                    for (int i = 0; i < length / 4; i++) {
////                        lists.add(Float.intBitsToFloat(getInt(buffer, 4 * i)));
////                    }
////                } else {
////                    for (int i = 0; i < length / 4; i++) {
////                        lists.set(i, Float.intBitsToFloat(getInt(buffer, 4 * i)) + lists.get(i));
////                    }
////                }
//
//            }
//            String result = "";
//            for (int i = 0; i < lists.size(); i++) {
//                result += lists.get(i) + ",";
//            }
//            //System.out.println("result:" + result);
//            result = result.substring(0, result.lastIndexOf(","));
//            context.write(key, new Text(result));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }

    public int getInt(byte[] bytes, int offset) {
        return (0xff & bytes[0 + offset]) | (0xff00 & (bytes[1 + offset] << 8))
                | (0xff0000 & (bytes[2 + offset] << 16))
                | (0xff000000 & (bytes[3 + offset] << 24));
    }

    /**
     * Called once at the end of the task.
     *
     * @param context
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
