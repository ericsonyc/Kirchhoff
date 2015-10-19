package hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ericson on 2015/3/4 0004.
 */
public class MyMapper extends Mapper<IntWritable, Text, Text, Text> {
    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
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
            System.out.println("begin map");
            long start = System.currentTimeMillis();
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
                System.out.println("place:" + i);
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
                // System.out.println("keytemp:"+keytemp[0]);
                Thread thread = new Thread(new OutputShotThread(keytemp, fcxy, context.getConfiguration(), maplists));
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

//			while (true) {
//				if (!OutputShotThread.hasRunningThread()) {
//					threads = null;
//					// System.gc();
//					break;
//				} else {
//					Thread.currentThread().sleep(500);
//				}
//			}

            long end = System.currentTimeMillis();
            System.out.println("OutputTime:" + (end - start) + "ms");

//			Iterator iter = maps.entrySet().iterator();
//			while (iter.hasNext()) {
//				Entry entry = (Entry) iter.next();
//				int entrykey = (Integer) entry.getKey();
//				float[] entryvalue = (float[]) entry.getValue();
//				String result = "";
//				for (int j = 0; j < entryvalue.length; j++) {
//					result += entryvalue[j] + ",";
//				}
//				result = result.substring(0, result.lastIndexOf(","));
//				context.write(new Text(String.valueOf(entrykey)), new Text(
//						result));
//			}
            int mapLength = 0;
            List<Thread> threadlists = new ArrayList<Thread>();
            for (int j = 0; j < place; j++) {
                mapLength += maplists[j].size();
                Thread thread = new ContextWrite(maplists[j], context);
                threadlists.add(thread);
                thread.start();
            }

            for (int j = 0; j < place; j++) {
                threadlists.get(j).join();
            }
            long end1 = System.currentTimeMillis();
            System.out.println("maplength:" + mapLength);
            System.out.println("mapTime:" + (end1 - end) + "ms");
            threadlists = null;
            System.gc();
            // String keyStr="";
            // for(int i=0;i<keys.length;i++){
            // keyStr+=String.valueOf(keys[i])+",";
            // }
            // LOG.info("key: "+keyStr);
            // LOG.info("value: "+value.toString());
            // LOG.info("length: "+datas.length);
            // context.write(new Text(String.valueOf(key.get())), value);
            // context.write(new Text(String.valueOf(key)), value);
        } catch (Exception e) {
            System.out.println("Exception in the map fuction");
            e.printStackTrace();
        }
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
            // TODO Auto-generated method stub
            //System.out.println("write:");
            System.out.println("maps.length:" + maps.size());
            Text textKey = new Text();
            Text textValue = new Text();
            for (Map.Entry<Integer, float[]> entry : maps.entrySet()) {
                int entryKey = entry.getKey();
                float[] value = entry.getValue();
                //System.out.println("result:" + entryKey + "," + value[0] + "," + value[value.length / 2]);
                String result = "";
                for (int i = 0; i < value.length; i++) {
                    result += value[i] + ",";
                }
                result = result.substring(0, result.lastIndexOf(","));
                try {
                    long start = System.currentTimeMillis();
                    textKey.set(String.valueOf(entryKey));
                    textValue.set(result);
                    context.write(textKey, textValue);
                    long end = System.currentTimeMillis();
                    //System.out.println("map write:" + (end - start) + "ms");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
    }
}
