package hdfswrite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class CPUKTMigrationMapWriteHdfs extends Mapper<IntWritable, Text, Text, Text> {

    public int getInt(byte[] bytes) {
        return (0xff & bytes[0]) | (0xff00 & (bytes[1] << 8))
                | (0xff0000 & (bytes[2] << 16))
                | (0xff000000 & (bytes[3] << 24));
    }

    public void read(float[] outputDataBuffer, KTFile ktFile) {
        try {
            DataInputStream dis = new DataInputStream(new BufferedInputStream(
                    new FileInputStream(ktFile.getFilePath())));
            for (int i = 0; i < outputDataBuffer.length; i++) {
                byte[] temp = new byte[4];
                dis.read(temp);
                outputDataBuffer[i] = Float.intBitsToFloat(this.getInt(temp));
            }
            dis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void map(IntWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        String[] datas = value.toString().split(",");
        int[] keys = new int[datas.length / 2];
        float[] fcxydatas = new float[datas.length];
        for (int i = 0; i < datas.length / 2; i++) {
            keys[i] = key.get() + i;
            fcxydatas[2 * i] = Float.parseFloat(datas[2 * i]);
            fcxydatas[2 * i + 1] = Float.parseFloat(datas[2 * i + 1]);
        }
        datas = null;
        long end = System.currentTimeMillis();
        System.out.println("get key value Time:" + (end - start) + "ms");
        try {


            // this.getOuputShot(keys, fcxydatas, context);

            //HashMap<Integer, float[]> maps = new HashMap<Integer, float[]>();

            List<Thread> threads = new ArrayList<Thread>();
            int place = 4;
            HashMap<Integer, float[]>[] maplists = new HashMap[place];
            for (int i = 0; i < place; i++) {
                maplists[i] = new HashMap<Integer, float[]>();
            }
            int count = keys.length / place;
            for (int i = 0; i < place; i++) {
                System.out.println("thread:" + i);
                int[] keytemp = null;
                float[] fcxy = null;
                if (i == place - 1) {
                    keytemp = new int[keys.length - (place - 1) * count];
                    fcxy = new float[2 * keytemp.length];

                } else {
                    keytemp = new int[count];
                    fcxy = new float[2 * keytemp.length];

                }
                int con = i * count;
                System.out.println("con:" + con);
                for (int j = 0; j < keytemp.length; j++) {
                    keytemp[j] = keys[con + j];
                    fcxy[2 * j] = fcxydatas[2 * (con + j)];
                    fcxy[2 * j + 1] = fcxydatas[2 * (con + j) + 1];
                }
                start = System.currentTimeMillis();
                OutputShotThread out = new OutputShotThread(keytemp, fcxydatas, context.getConfiguration(), maplists);
                end = System.currentTimeMillis();
                System.out.println("outputshotthread time:" + (end - start) + "ms");
                Thread thread = new Thread(out);
                threads.add(thread);
                thread.start();
            }
            keys = null;
            fcxydatas = null;

            // for (int i = 0; i < threads.size(); i++) {
            // threads.get(i).start();
            // }

            for (int i = 0; i < threads.size(); i++) {
                threads.get(i).join();
            }
            threads = null;

            end = System.currentTimeMillis();
            System.out.println("All threads time:" + (end - start) + "ms");

            start = end;
            Configuration conf = context.getConfiguration();
            String taskID = context.getTaskAttemptID().toString();
            String filename = taskID.substring(taskID.lastIndexOf("m"));
            String path = conf.get("path");
            Path output = new Path(path + "/map/" + filename);
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FSDataOutputStream fsOut = fs.create(output, true);
            long offset = 0;
            int len = conf.getInt("ont", 1000);
            byte[] bytes = new byte[len * 4];
            for (int i = 0; i < maplists.length; i++) {
                HashMap<Integer, float[]> maps = maplists[i];
                for (Entry<Integer, float[]> en : maps.entrySet()) {
                    int entryKey = en.getKey();
//                    System.out.println("entryKey:" + entryKey);
                    float[] vv = en.getValue();
                    for (int j = 0; j < vv.length; j++) {
                        getBytes(Float.floatToIntBits(vv[j]), bytes, j * 4);
                    }
//                    System.out.println("vv.length:" + vv.length);
//                    System.out.println("entryValue:" + vv[0] + "," + vv[vv.length / 2]);
                    fsOut.write(bytes, 0, vv.length * Float.SIZE / 8);
                    fsOut.flush();
                    context.write(new Text(String.valueOf(entryKey)), new Text(filename + "#" + offset + "#" + vv.length));
//                    System.out.println("offset:" + offset);
                    offset += vv.length * Float.SIZE / 8;
                }
            }
            fsOut.close();

            end = System.currentTimeMillis();
            System.out.println("write hdfs time:" + (end - start) + "ms");

        } catch (Exception e) {
            System.out.println("Exception in the map fuction");
            e.printStackTrace();
        }
    }

    public void getBytes(int data, byte[] bytes, int offset) {
        bytes[offset] = (byte) (data & 0xff);
        bytes[1 + offset] = (byte) ((data & 0xff00) >> 8);
        bytes[2 + offset] = (byte) ((data & 0xff0000) >> 16);
        bytes[3 + offset] = (byte) ((data & 0xff000000) >> 24);
    }

    class ContextWrite extends Thread {

        private HashMap<Integer, float[]> maps = null;
        private Context context = null;

        public ContextWrite(HashMap<Integer, float[]> maps, Context context) {
            this.maps = maps;
            this.context = context;
        }

        @Override
        public void run() {
            // TODO auto-generated method stub
            System.out.println("write:");
            for (Entry<Integer, float[]> entry : maps.entrySet()) {
                int entryKey = entry.getKey();
                float[] value = entry.getValue();
//                String result = "";
//                for (int i = 0; i < value.length; i++) {
//                    result += value[i] + ",";
//                }
//                result = result.substring(0, result.lastIndexOf(","));
                try {

                    String taskID = context.getTaskAttemptID().toString();
                    String filename = taskID.substring(taskID.lastIndexOf("m"));
//                    System.out.println("key:" + entryKey + ",value:" + value[0] + ",filename:" + filename);
                    long offset = this.writeFile(entryKey, value, filename);
                    context.write(new Text(String.valueOf(entryKey)), new Text(filename + "#" + offset + "#" + value.length * 4));
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }

        private synchronized long writeFile(int key, float[] value, String filename) {
            long offset = 0;
            try {
//                System.out.println("begin writeFile");
                FileSystem fs = FileSystem.get(context.getConfiguration());
                String path = context.getConfiguration().get("path");
                Path output = new Path(path + "map/" + filename);
                int length = 0;
//                System.out.println("detect the file exists or not");
                if (fs.exists(output)) {
//                    System.out.println("output exists");
                    FSDataInputStream fsIn = fs.open(output);
                    byte[] temp = new byte[2000];
                    while ((length = fsIn.read(temp)) != -1) {
                        offset += length;
                    }
                    fsIn.close();
                }
//                System.out.println("writeFile,fileoffset:" + offset);
                FSDataOutputStream fsOut = null;
                if (fs.exists(output)) {
                    fsOut = fs.append(output);
                } else {
                    fsOut = fs.create(output);
                }
                byte[] test = new byte[value.length * 4];
                for (int i = 0; i < value.length; i++) {
                    this.getBytes(Float.floatToIntBits(value[i]), test, 4 * i);
                }
                fsOut.write(test);
                fsOut.close();

            } catch (Exception e) {
                e.printStackTrace();
            }

            return offset;
        }

        public void getBytes(int data, byte[] bytes, int offset) {
            bytes[offset] = (byte) (data & 0xff);
            bytes[1 + offset] = (byte) ((data & 0xff00) >> 8);
            bytes[2 + offset] = (byte) ((data & 0xff0000) >> 16);
            bytes[3 + offset] = (byte) ((data & 0xff000000) >> 24);
        }
    }

}
