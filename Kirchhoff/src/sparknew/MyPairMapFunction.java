package sparknew;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyPairMapFunction implements
        PairFlatMapFunction<Tuple2<Integer, String>, Integer, String> {

    // private static List<Tuple2<String, String>> arrays = new
    // ArrayList<Tuple2<String, String>>();
    private ProgramConf pconf = null;
    private static String filename = "/map.file";

    // private HashMap<Integer, float[]> maps = null;

    public MyPairMapFunction(ProgramConf pconf) {
        this.pconf = pconf;
    }

    @Override
    public Iterable<Tuple2<Integer, String>> call(Tuple2<Integer, String> tuples)
            throws Exception {
        // TODO Auto-generated method stub
        System.out.println("filename:" + filename);
        List<Tuple2<Integer, String>> arrays = new ArrayList<Tuple2<Integer, String>>();
        long start = System.currentTimeMillis();
        String[] datas = tuples._2().split(",");
        int[] keys = new int[datas.length / 2];
        // int key = tuples._1().intValue();
        float[] fcxydatas = new float[datas.length];
        // System.out.println(tuples._1().intValue());
        for (int i = 0; i < datas.length / 2; i++) {
            keys[i] = tuples._1().intValue() + i;
            fcxydatas[2 * i] = Float.parseFloat(datas[2 * i]);
            fcxydatas[2 * i + 1] = Float.parseFloat(datas[2 * i + 1]);
            // fcxydatas[i] = Float.parseFloat(datas[i]);
        }
        long end = System.currentTimeMillis();
//        System.out.println("pre handle time:" + (end - start) + "ms");
//        System.out.println("keys.length:" + keys.length);
//        System.out.println("keys:" + keys[0] + ",value:" + fcxydatas[0] + "," + fcxydatas[1]);
        try {

            // Logger.getLogger("Spark").info("begin map");
//            System.out.println("begin map");

            // List<Thread> threads = new ArrayList<Thread>();
            // int place = 4;
            // HashMap<Integer, float[]> maps = new HashMap<Integer, float[]>();
            // int count = keys.length / place;
            // for (int i = 0; i < place; i++) {
            // System.out.println("place:" + place);
            // int[] keytemp = null;
            // float[] fcxy = null;
            // if (i == place - 1) {
            // keytemp = new int[keys.length - (place - 1) * count];
            // fcxy = new float[2 * keytemp.length];
            //
            // } else {
            // keytemp = new int[count];
            // fcxy = new float[2 * keytemp.length];
            //
            // }
            // int con = i * count;
            // System.out.println("con:" + con);
            // for (int j = 0; j < keytemp.length; j++) {
            // keytemp[j] = keys[con + j];
            // fcxy[2 * j] = fcxydatas[2 * (con + j)];
            // fcxy[2 * j + 1] = fcxydatas[2 * (con + j) + 1];
            // }
            // Thread thread = new Thread(new OutputShotThread(keytemp, fcxy,
            // maps, pconf));
            // threads.add(thread);
            // thread.start();
            // }
            //
            // for (int i = 0; i < threads.size(); i++) {
            // threads.get(i).join();
            // }
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
                Thread thread = new Thread(new OutputShotThread(keytemp, fcxy, pconf, maplists));
                threads.add(thread);
                thread.start();
            }
            keys = null;
            fcxydatas = null;

            for (int i = 0; i < threads.size(); i++) {
                threads.get(i).join();
            }
            threads = null;


//            start = System.currentTimeMillis();
//            Configuration conf = new Configuration();
//            Path path = new Path(pconf.getPath() + filename);
//            FileSystem fs = FileSystem.get(conf);
//            if (fs.exists(path)) {
//                fs.delete(path, true);
//            }
//            try {
//                FSDataOutputStream fsOut = fs.create(path, true);
//                long offset = 0;
//                int len = pconf.getOnt();
//                byte[] bytes = new byte[len * 4];
//                for (int i = 0; i < maplists.length; i++) {
//                    HashMap<Integer, float[]> maps = maplists[i];
//                    for (Map.Entry<Integer, float[]> en : maps.entrySet()) {
//                        int entryKey = en.getKey();
//                        float[] vv = en.getValue();
//                        for (int j = 0; j < vv.length; j++) {
//                            getBytes(Float.floatToIntBits(vv[j]), bytes, j * 4);
//                        }
//                        fsOut.write(bytes, 0, vv.length * Float.SIZE / 8);
//                        fsOut.flush();
//                        arrays.add(new Tuple2<Integer, String>(entryKey, filename + "#" + offset));
//                        offset += vv.length * Float.SIZE / 8;
//                    }
//                }
//                fsOut.close();
//                end = System.currentTimeMillis();
//                System.out.println("write hdfs file time:" + (end - start) + "ms");
//            } catch (Exception e) {
//                e.printStackTrace();
//            }


            List<Thread> threadlists = new ArrayList<Thread>();
            for (int j = 0; j < place; j++) {
//                mapLength += maplists[j].size();
                Thread thread = new ContextWrite(maplists[j], arrays, j);
                threadlists.add(thread);
                thread.start();
            }

            for (int j = 0; j < place; j++) {
                threadlists.get(j).join();
            }
            threadlists = null;


        } catch (Exception e) {
            System.out.println("Exception in the map fuction");
            e.printStackTrace();
        }
        return arrays;
    }

    public void getBytes(int data, byte[] bytes, int offset) {
        bytes[offset] = (byte) (data & 0xff);
        bytes[1 + offset] = (byte) ((data & 0xff00) >> 8);
        bytes[2 + offset] = (byte) ((data & 0xff0000) >> 16);
        bytes[3 + offset] = (byte) ((data & 0xff000000) >> 24);
    }

    class ContextWrite extends Thread {

        private HashMap<Integer, float[]> maps = null;
        private List<Tuple2<Integer, String>> tupleLists = null;

        private int threadID = 0;

        public ContextWrite(HashMap<Integer, float[]> maps, List<Tuple2<Integer, String>> tupleLists, int j) {
            this.maps = maps;
            this.tupleLists = tupleLists;
            this.threadID = j;
        }

        @Override
        public void run() {
            // TODO Auto-generated method stub
            //System.out.println("write:");
//            System.out.println(threadID + "maps.length:" + maps.size());
            for (Map.Entry<Integer, float[]> entry : maps.entrySet()) {
                int entryKey = entry.getKey();
                float[] value = entry.getValue();
                //System.out.println("result:" + entryKey + "," + value[0] + "," + value[value.length / 2]);
                String result = "";
                for (int i = 0; i < value.length; i++) {
                    result += value[i] + ",";
                }
                result = result.substring(0, result.lastIndexOf(","));
                synchronized (tupleLists) {
                    tupleLists.add(new Tuple2<Integer, String>(entryKey, result));
                }
//                System.out.println(threadID + "outputkeys:" + entryKey + ",value:" + value[0] + "," + value[value.length - 1]);
            }
        }
    }

    // private void getOuputShot(int[] keys, float[] cxcy) throws Exception {
    // apx = pconf.getApx();
    // apy = pconf.getApy();
    // beVerb = pconf.isBeVerb();
    // onx = pconf.getOnx();
    // ony = pconf.getOny();
    // ont = pconf.getOnt();
    // nt = pconf.getNt();
    // oot = pconf.getOot();
    // odt = pconf.getOdt();
    // dt = pconf.getDt();
    // oox = pconf.getOox();
    // odx = pconf.getOdx();
    // ooy = pconf.getOoy();
    // ody = pconf.getOdy();
    // ot = pconf.getOt();
    // beDiff = pconf.isBeDiff();
    // beAntiAliasing = pconf.isBeAntiAliasing();
    // maxtri = pconf.getMaxtri();
    // trfact = pconf.getTrfact();
    // String PATH = pconf.getPath();
    // int[] ap = new int[onx * ony];
    // // System.out.println("getOuputShot:apx:" + apx + " ,apy:" + apy);
    // // ArrayList<Tuple2<String, String>> array = new
    // // ArrayList<Tuple2<String, String>>();
    // long start = System.currentTimeMillis();
    // float[] velocity = new float[ont * onx * ony];
    // this.readData(PATH + "data/rmsv.data", ont, 0, Float.SIZE / 8, velocity);
    // float[] trace = new float[nt * keys.length];
    // float[] sxsy = new float[2 * keys.length];
    // float[] gxgy = new float[2 * keys.length];
    //
    // // FileSystem fs = FileSystem.get(pconf.getConf());
    // this.readData(PATH + "data/shot.data", nt, keys[0], Float.SIZE / 8,
    // trace);
    // this.readData(PATH + "data/fsxy.data", 2, keys[0], Float.SIZE / 8, sxsy);
    // this.readData(PATH + "data/fgxy.data", 2, keys[0], Float.SIZE / 8, gxgy);
    // long end = System.currentTimeMillis();
    // System.out.println("readTime:" + (end - start) + "ms");
    //
    // // 计算输入道在输出结果中的位置
    //
    // for (int i = 0; i < keys.length; ++i) {
    // long start1 = System.currentTimeMillis();
    // int minix = onx - 1;
    // int maxix = 0;
    // int miniy = ony - 1;
    // int maxiy = 0;
    // int ix, iy;
    // // 计算输入道在输出结果中的位置
    // ix = (int) ((cxcy[2 * i] - oox) / odx + 0.5f);// 计算idxi
    // iy = (int) ((cxcy[2 * i + 1] - ooy) / ody + 0.5f);// 计算idyi
    // if (ix < minix) {
    // minix = ix;
    // }
    // if (ix > maxix) {
    // maxix = ix;
    // }
    // if (iy < miniy) {
    // miniy = iy;
    // }
    // if (iy > maxiy) {
    // maxiy = iy;
    // }
    // // System.out.println("test");
    // // Aperture corners，求出aox,aoy,aex,aey
    // // 计算孔径范围，el_cx1代表输出道
    // int el_cx1 = minix;
    // int el_cx2 = maxix;
    // int el_cy1 = miniy;
    // int el_cy2 = maxiy;
    //
    // minix -= apx;
    // if (minix < 0) {
    // minix = 0;
    // }
    // miniy -= apy;
    // if (miniy < 0) {
    // miniy = 0;
    // }
    // maxix += apx;
    // if (maxix >= onx) {
    // maxix = onx - 1;
    // }
    // maxiy += apy;
    // if (maxiy >= ony) {
    // maxiy = ony - 1;
    // }
    //
    // // if (beVerb) {
    // // // System.out.println("key:" + keys[i]);
    // // System.out.println("Rectangular aperture: " + minix + "-"
    // // + maxix + "," + miniy + "-" + maxiy);
    // // // Logger.getLogger("beVerb").info(
    // // // "Rectangular aperture:" + minix + "-" + maxix + ","
    // // // + miniy + "-" + maxiy);
    // // }
    //
    // int l = 0;
    // // 计算输出道
    // // String outputshot = "";
    // int iiy, iix, oidx;
    // for (iiy = miniy; iiy <= maxiy; ++iiy) {
    // for (iix = minix; iix <= maxix; ++iix) {
    // oidx = iiy * onx + iix;
    // if ((iix >= el_cx1 && iix <= el_cx2)
    // && (iiy >= el_cy1 && iiy <= el_cy2)) {
    // ap[l] = oidx;
    // // outputshot += String.valueOf(ap[l]) + ",";
    // ++l;
    // continue;
    // }
    //
    // float el_x = (iix < el_cx1) ? iix - el_cx1 : iix - el_cx2;
    // float el_y = (iiy < el_cy1) ? iiy - el_cy1 : iiy - el_cy2;
    // // Check if the point is within one of the ellipses
    // if ((el_x * el_x / (apx * apx) + (el_y * el_y)
    // / (apy * apy)) < 1.0f) {
    // ap[l] = oidx;
    // // outputshot += String.valueOf(ap[l]) + ",";
    // ++l;
    // }
    // }// for ix
    // }// for iy
    // // System.out.println("l:"+l);
    // // this.computOutput(conf, keys[i], ap, l, context);
    // try {
    //
    // // fs.close();
    // float sx = sxsy[2 * i];
    // float sy = sxsy[2 * i + 1];
    // float gx = gxgy[2 * i];
    // float gy = gxgy[2 * i + 1];
    // // ktMigSbDiff---beDiff
    // if (beDiff) {
    // this.ktMigSbDiff(trace, nt, i, dt);
    // }
    //
    // if (beAntiAliasing) {
    // this.ktMigCint(trace, nt, i);
    // this.ktMigAcint(trace, nt, i);
    // }
    //
    // System.out.println("Inputkey:" + keys[i]);
    // // System.out.println("trace:" + Str);
    // // System.exit(0);
    // long start2 = System.currentTimeMillis();
    // for (int t = 0; t < l; t++) {
    // float[] image = new float[ont];
    // // fs = FileSystem.get(conf);
    // // read the velocity file
    //
    // float ox = oox + (ap[t] % onx) * odx;
    // float oy = ooy + (ap[t] / onx) * ody;
    // this.ktMigKernel(trace, i, velocity, ap[t], image, ox, oy,
    // sx, sy, gx, gy, nt, ont, ot, dt, oot, odt, maxtri,
    // trfact, beAntiAliasing);
    //
    // if (maps.containsKey(ap[t])) {
    // float[] value = maps.get(ap[t]);
    // for (int j = 0; j < ont; j++) {
    // value[j] += image[j];
    // }
    // maps.put(ap[t], value);
    // } else {
    // maps.put(ap[t], image);
    // }
    // image = null;
    // }
    // long end2 = System.currentTimeMillis();
    // System.out.println("outputTime:" + (end2 - start2) + "ms");
    // // fs.close();
    // // System.gc();
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    // long end1 = System.currentTimeMillis();
    // System.out.println("keyTime:" + (end1 - start1) + "ms");
    // }
    //
    // // System.out.println("the length of the array---ap is: "+l);
    // // for (int t_index = 0; t_index < l; t_index++) {
    // // int oidx_temp = ap[t_index];
    // // // System.out.println("key,value:" + String.valueOf(oidx_temp)
    // // // + "," + String.valueOf(keys[i]));
    // // context.write(new Text(String.valueOf(oidx_temp)), new Text(
    // // String.valueOf(keys[i])));
    // // }
    //
    // // context.write(new Text(outputshot), new
    // // Text(String.valueOf(keys[i])));
    //
    // // }
    //
    // // for (int i = 0; i < l; i++) {
    // // array.add(new Tuple2<String, String>(String.valueOf(ap[i]), String
    // // .valueOf(key)));
    // // }
    // velocity = null;
    // trace = null;
    // sxsy = null;
    // gxgy = null;
    // // System.gc();
    // }

    private void readData(String path, int length, int start, int SIZE,
                          float[] datas) {
        try {
            // System.out.println(path);
            Path filepath = new Path(path);
            FileSystem fs = filepath.getFileSystem(new Configuration());
            FSDataInputStream fileIn = fs.open(filepath);
            fileIn.seek(length * start * SIZE);
            byte[] temp = new byte[4];
            // String result = "";
            for (int i = 0; i < datas.length; i++) {
                fileIn.read(temp);
                datas[i] = Float.intBitsToFloat(getInt(temp));
                // result += datas[i] + ",";
            }
            // System.out.println(result);
            fileIn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int getInt(byte[] bytes) {
        return (0xff & bytes[0]) | (0xff00 & (bytes[1] << 8))
                | (0xff0000 & (bytes[2] << 16))
                | (0xff000000 & (bytes[3] << 24));
    }

    private void ktMigKernel(float[] trace, int traceindex, float[] vrms,
                             int vrmsindex, float[] image, float ox, float oy, float sx,
                             float sy, float gx, float gy, int nt, int ont, float ot, float dt,
                             float oot, float odt, int trm, float trf, boolean aa) {

        float v, inv;
        float inv2trf, nf;
        float j, scale, smp, so2, go2;
        float depth2, dx, dy, ts, tg;
        int traceposition = nt * traceindex;
        int vrmsposition = ont * vrmsindex;

        // Loop over tau indices
        for (int k = 0; k < ont; ++k) {
            // RMS velocity at image location
            v = vrms[vrmsposition + k];
            // Slowness at image location
            inv = 1.0f / v;
            inv2trf = trf * inv * inv;
            depth2 = (float) Math.pow(0.5 * v * (oot + k * odt), 2.0);
            // squared distance to source from the image point on the surface
            so2 = (sx - ox) * (sx - ox) + (sy - oy) * (sy - oy);
            // squared distance to receiver from the image point on the surface
            go2 = (gx - ox) * (gx - ox) + (gy - oy) * (gy - oy);
            // Time from source to image point in pseudodepth
            ts = (float) Math.sqrt(so2 + depth2) * inv;
            // Time from receiver to image point in pseudodepth
            tg = (float) Math.sqrt(go2 + depth2) * inv;
            // double root square time = time to source + time to receiver
            j = (ts + tg - ot) / dt;

            if (!aa) {
                if (j >= 0.f && j < nt - 1) {
                    image[k] += INTSMP(trace, j, nt, traceposition);
                }
                continue;
            }
            // (distance to source.x) / (time to source) + (distance to
            // receiver.x) / (time to receiver)
            dx = (sx - ox) / ts + (gx - ox) / tg;
            // (distance to source.y) / (time to source) + (distance to
            // receiver.y) / (time to receiver)
            dy = (sy - oy) / ts + (gy - oy) / tg;
            // Filter length
            nf = (float) (inv2trf * Math.sqrt(dx * dx + dy * dy));
            // Truncate filter
            if (nf > trm) {
                nf = (float) trm;
            }
            // Check ranges
            if ((j - nf - 1.0f) >= 0.0f && (j + nf + 1.0f) < nt) {
                // Scaling factor
                scale = 1.0f / (1.0f + nf);
                scale *= scale;
                // Collect samples
                smp = 2.0f * INTSMP(trace, j, nt, traceposition)
                        - INTSMP(trace, (j - nf - 1.0f), nt, traceposition)
                        - INTSMP(trace, (j + nf + 1.0f), nt, traceposition);
                // Contribute to the image point
                image[k] += scale * smp;
            }
        }
    }

    private float INTSMP(float[] t, float i, int length, int traceposition) {
        float out;
        float out1;
        if ((int) i + 1 >= length) {
            out1 = 0;
        } else {
            out1 = t[traceposition + (int) (i) + 1];
        }
        if ((int) i >= length) {
            out = 0;
        } else {
            out = t[traceposition + (int) i];
        }
        float value = ((1.0f - i + (float) ((int) i)) * out + (i - (float) ((int) i))
                * out1);
        return value;
    }

    private void ktMigCint(float[] trace, int nt, int index) {
        int count = nt * index;
        for (int i = 1; i < nt; ++i) {
            trace[count + i] += trace[count + i - 1];
        }
    }

    private void ktMigAcint(float[] trace, int nt, int index) {
        int count = nt * index;
        for (int i = nt - 2; i >= 0; i--) {
            trace[count + i] += trace[count + i + 1];
        }
    }

    private void ktMigSbDiff(float[] trace, int length, int index,
                             float distance) {
        float val0, val1, val2;
        int count = length * index;
        val1 = trace[count];
        val2 = trace[count];

        for (int i = 0; i < length; ++i) {
            val0 = trace[count + i];
            trace[count + i] = 0.5f * (3.0f * val0 - 4.0f * val1 + val2)
                    / distance;
            val2 = val1;
            val1 = val0;
        }
    }
}
