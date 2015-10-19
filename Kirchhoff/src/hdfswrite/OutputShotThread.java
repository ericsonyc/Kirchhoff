package hdfswrite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;

class OutputShotThread implements Runnable {
    private boolean beAntiAliasing;
    private boolean beDiff;
    private boolean beVerb;
    private int apx;
    private int apy;
    private int maxtri;
    private float trfact;
    private int onx;
    private int ony;
    private int ont;
    private int nt;
    private float oot;
    private float odt;
    private float dt;
    private float oox;
    private float odx;
    private float ooy;
    private float ody;
    private float ot;

    private int[] keys = null;
    private float[] fcxy = null;
    private Configuration conf = null;
    private HashMap<Integer, float[]>[] maps = null;
    private int CONSTANT = 1500;
    private int LENGTH;


    public OutputShotThread(int[] keys, float[] fcxy, Configuration conf,
                            HashMap<Integer, float[]>[] maps) {
        this.keys = keys;
        this.fcxy = fcxy;
        this.conf = conf;
        this.maps = maps;
        this.setValue();
    }

    private void setValue() {
        apx = conf.getInt("apx", 20);
        apy = conf.getInt("apy", 20);
        beVerb = conf.getBoolean("beVerb", true);
        onx = conf.getInt("onx", 1);
        ony = conf.getInt("ony", 1);
        ont = conf.getInt("ont", 1);
        nt = conf.getInt("nt", 1);
        oot = conf.getFloat("oot", 1.0f);
        odt = conf.getFloat("odt", 1.0f);
        dt = conf.getFloat("dt", 1.0f);
        oox = conf.getFloat("oox", 1.0f);
        odx = conf.getFloat("odx", 1.0f);
        ooy = conf.getFloat("ooy", 1.0f);
        ody = conf.getFloat("ody", 1.0f);
        ot = conf.getFloat("ot", 1.0f);
        beDiff = conf.getBoolean("beDiff", true);
        beAntiAliasing = conf.getBoolean("beAntiAliasing", true);
        maxtri = conf.getInt("maxtri", 1);
        trfact = conf.getFloat("trfact", 1.0f);

    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        try {
            long start = System.currentTimeMillis();
            getOuputShot();
            long end = System.currentTimeMillis();
            System.out.println("thread run time:" + (end - start) + "ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void getOuputShot() throws Exception {
        String PATH = conf.get("path");
        FileSystem fs = FileSystem.get(conf);
        int[] ap = new int[onx * ony];
        float[] velocity = null;
        float[] trace = new float[nt * keys.length];
        float[] sxsy = new float[2 * keys.length];
        float[] gxgy = new float[2 * keys.length];
        long start = System.currentTimeMillis();
        this.readData(PATH + "/shot.data", nt, keys[0], trace.length, Float.SIZE / 8,
                trace, fs);
        this.readData(PATH + "/fsxy.data", 2, keys[0], sxsy.length, Float.SIZE / 8,
                sxsy, fs);
        this.readData(PATH + "/fgxy.data", 2, keys[0], gxgy.length, Float.SIZE / 8,
                gxgy, fs);
        long end = System.currentTimeMillis();
        System.out.println("readData time:" + (end - start) + "ms");
//        float[] image = new float[ont * CONSTANT];
        byte[] datafully = new byte[Float.SIZE / 8 * CONSTANT * ont];
        velocity = new float[ont * CONSTANT];
        for (int i = 0; i < keys.length; ++i) {
            long sss = System.currentTimeMillis();
            int minix = onx - 1;
            int maxix = 0;
            int miniy = ony - 1;
            int maxiy = 0;
            int ix, iy;
            // 计算输入道在输出结果中的位置
            ix = (int) ((fcxy[2 * i] - oox) / odx + 0.5f);// 计算idxi
            iy = (int) ((fcxy[2 * i + 1] - ooy) / ody + 0.5f);// 计算idyi
            if (ix < minix) {
                minix = ix;
            }
            if (ix > maxix) {
                maxix = ix;
            }
            if (iy < miniy) {
                miniy = iy;
            }
            if (iy > maxiy) {
                maxiy = iy;
            }
            // System.out.println("test");
            // Aperture corners，求出aox,aoy,aex,aey
            // 计算孔径范围，el_cx1代表输出道
            int el_cx1 = minix;
            int el_cx2 = maxix;
            int el_cy1 = miniy;
            int el_cy2 = maxiy;

            minix -= apx;
            if (minix < 0) {
                minix = 0;
            }
            miniy -= apy;
            if (miniy < 0) {
                miniy = 0;
            }
            maxix += apx;
            if (maxix >= onx) {
                maxix = onx - 1;
            }
            maxiy += apy;
            if (maxiy >= ony) {
                maxiy = ony - 1;
            }

            // if (beVerb) {
            // // System.out.println("key:" + keys[i]);
            // System.out.println("Rectangular aperture: " + minix + "-"
            // + maxix + "," + miniy + "-" + maxiy);
            // // Logger.getLogger("beVerb").info(
            // // "Rectangular aperture:" + minix + "-" + maxix + ","
            // // + miniy + "-" + maxiy);
            // }

            int l = 0;
            // 计算输出道
            // String outputshot = "";
            int iiy, iix, oidx;
            for (iiy = miniy; iiy <= maxiy; ++iiy) {
                for (iix = minix; iix <= maxix; ++iix) {
                    oidx = iiy * onx + iix;
                    if ((iix >= el_cx1 && iix <= el_cx2)
                            && (iiy >= el_cy1 && iiy <= el_cy2)) {
                        ap[l] = oidx;
                        // outputshot += String.valueOf(ap[l]) + ",";
                        ++l;
                        continue;
                    }

                    float el_x = (iix < el_cx1) ? iix - el_cx1 : iix - el_cx2;
                    float el_y = (iiy < el_cy1) ? iiy - el_cy1 : iiy - el_cy2;
                    // Check if the point is within one of the ellipses
                    if ((el_x * el_x / (apx * apx) + (el_y * el_y)
                            / (apy * apy)) < 1.0f) {
                        ap[l] = oidx;
                        // outputshot += String.valueOf(ap[l]) + ",";
                        ++l;
                    }
                }// for ix
            }// for iy
            this.quick_sort(ap, 0, l - 1);
            // this.computOutput(conf, keys[i], ap, l, context);
            try {

                float sx = sxsy[2 * i];
                float sy = sxsy[2 * i + 1];
                float gx = gxgy[2 * i];
                float gy = gxgy[2 * i + 1];
                // ktMigSbDiff---beDiff
                if (beDiff) {
                    this.ktMigSbDiff(trace, nt, i, dt);
                }

                if (beAntiAliasing) {
                    this.ktMigCint(trace, nt, i);
                    this.ktMigAcint(trace, nt, i);
                }
                long eee = System.currentTimeMillis();
                System.out.println("pregetdata time:" + (eee - sss) + "ms");

//                System.out.println("**************Inputkey:" + keys[i]);
                // System.out.println("trace:" + Str);
                // System.exit(0);
                //velocity = new float[ont * l];
                //startTime = System.currentTimeMillis();
                //this.readPerVelocityData(velocity, ap, ont, l, PATH + "data/rmsv.data", fs, Float.SIZE / 8);
                //endTime = System.currentTimeMillis();
                //System.out.println("VelocityTime:" + (endTime - startTime) + "ms");

                if (l < CONSTANT) {
                    LENGTH = l;
                } else {
                    LENGTH = CONSTANT;
                }
                //System.out.println("LENGTH:" + LENGTH);
                long ssss = System.currentTimeMillis();
                readData(PATH + "/rmsv.data", datafully, ont, ap[0], LENGTH * ont, Float.SIZE / 8, velocity, fs);
                long eeee = System.currentTimeMillis();
                System.out.println("read velocity:" + (eeee - ssss) + "ms");

                int count = 0;
                int prev = count;
                int part = onx / maps.length;
                int indexx = 0;

//                for (int t = 0; t < l; ) {
//                    prev = count;
//                    System.out.println("prev:" + prev);
//                    while (ap[t] - ap[count] < LENGTH && t < l) {
//                        t++;
//                    }
//                    count = t;
////                    System.out.println("count:" + count + ",t:" + t);
//                    long sta = System.currentTimeMillis();
//                    this.ktMigKernel(trace, i, velocity, ap, prev, count, image, sx, sy, gx, gy);
//                    long ed = System.currentTimeMillis();
////                    System.out.println("kernel time:" + (ed - sta) + "ms");
//                    synchronized (maps) {
//                        for (int in = prev; in < count; in++) {
//                            indexx = ap[in] / part;
//                            if (indexx >= maps.length) {
//                                indexx = maps.length - 1;
//                            }
//                            HashMap<Integer, float[]> map = maps[indexx];
//                            if (map.containsKey(ap[in])) {
//                                float[] temp = map.get(ap[in]);
//                                for (int index = 0; index < temp.length; index++) {
//                                    temp[index] += image[index];
//                                }
//                                map.put(ap[in], temp);
//                                temp = null;
//                            } else {
//                                map.put(ap[in], image);
//                            }
//                            maps[indexx] = map;
//                        }
//
//                    }
//
//                    if (ap[count] - ap[prev] >= LENGTH) {
////                        System.out.println("change velocity");
//                        if (l - t < CONSTANT) {
//                            LENGTH = l - t;
//                        } else {
//                            LENGTH = CONSTANT;
//                        }
//                        //velocity = new float[ont * LENGTH];
//                        readData(PATH + "/rmsv.data", datafully, ont, ap[t], LENGTH * ont, Float.SIZE / 8, velocity, fs);
//                    }
//
//                    for (int in = 0; in < image.length; in++) {
//                        image[in] = 0;
//                    }
//                }
                start = System.currentTimeMillis();
                long start1 = 0;
                long end1 = 0;
                long kerneltime = 0;
                long start2 = 0;
                long end2 = 0;
                long maptime = 0;
                for (int t = 0; t < l; t++) {
                    float[] image = new float[ont];

                    float ox = oox + (ap[t] % onx) * odx;
                    float oy = ooy + (ap[t] / onx) * ody;
                    if (ap[t] - ap[count] >= LENGTH) {
                        if (l - t < CONSTANT) {
                            LENGTH = l - t;
                        } else {
                            LENGTH = CONSTANT;
                        }
                        //velocity = new float[ont * LENGTH];
                        readData(PATH + "/rmsv.data", datafully, ont, ap[t], LENGTH * ont, Float.SIZE / 8, velocity, fs);
                        count = t;
                        System.out.println("count LENGTH:" + LENGTH + ",ap[t]-ap[count]:" + (ap[t] - ap[count]));
                    }
//                    if (t % 1000 == 0)
//                        System.out.println("LENGTH:" + LENGTH);
//                    if (t % 1000 == 0)
//                        System.out.println("l:" + l + ",t:" + t + ",ap[t]-ap[count]:" + (ap[t] - ap[count]));

                    start1 = System.currentTimeMillis();

                    this.ktMigKernel(trace, i, velocity, ap[t] - ap[count], image, ox, oy,
                            sx, sy, gx, gy);

                    end1 = System.currentTimeMillis();
                    kerneltime += end1 - start1;

                    start2 = System.currentTimeMillis();
                    synchronized (maps) {
                        indexx = ap[t] / part;
                        if (indexx >= maps.length) {
                            indexx = maps.length - 1;
                        }
                        HashMap<Integer, float[]> map = maps[indexx];
                        if (map.containsKey(ap[t])) {
                            float[] temp = map.get(ap[t]);
                            for (int index = 0; index < temp.length; index++) {
                                temp[index] += image[index];
                            }
                            map.put(ap[t], temp);
                            temp = null;
                        } else {
                            map.put(ap[t], image);
                        }
                        maps[indexx] = map;
                    }
                    end2 = System.currentTimeMillis();
                    maptime += end2 - start2;

                    // context.write(new Text(String.valueOf(ap[t])), new Text(
                    // result));

                }
                System.out.println("kernel time:" + kerneltime + "ms");
                System.out.println("map time:" + maptime + "ms");
                end = System.currentTimeMillis();
                System.out.println("inputkey time:" + (end - start) + "ms");
            } catch (Exception e) {
                e.printStackTrace();
            }
            // long end1 = System.currentTimeMillis();
            // System.out.println("keyTime:" + (end1 - start1) + "ms");
        }
        // long start2 = System.currentTimeMillis();
        // Iterator iter = maps.entrySet().iterator();
        // while (iter.hasNext()) {
        // Entry entry = (Entry) iter.next();
        // int key = (Integer) entry.getKey();
        // float[] value = (float[]) entry.getValue();
        // String temp = "";
        // for (int i = 0; i < value.length; i++) {
        // temp += value[i] + ",";
        // }
        // temp = temp.substring(0, temp.lastIndexOf(","));
        // context.write(new Text(String.valueOf(key)), new Text(temp));
        // }
        velocity = null;
        trace = null;
        sxsy = null;
        gxgy = null;
        datafully = null;
        // long end2 = System.currentTimeMillis();
        // System.out.println("mapTime:" + (end2 - start2) + "ms");
        // System.gc();
    }

    private void ktMigKernel(float[] trace, int traceindex, float[] vrms, int[] ap, int prev, int count, float[] image, float sx, float sy, float gx, float gy) {
        float v, inv;
        float inv2trf, nf;
        float j, scale, smp, so2, go2;
        float depth2, dx, dy, ts, tg;
        int traceposition = nt * traceindex;
        int vrmsposition = 0;

        for (int t = prev; t < count; t++) {
            float ox = oox + (ap[t] % onx) * odx;
            float oy = ooy + (ap[t] / onx) * ody;
            vrmsposition = (t - prev) * ont;
            for (int k = 0; k < ont; ++k) {
                // RMS velocity at image location
                v = vrms[vrmsposition + k];
                // Slowness at image location
                inv = 1.0f / v;
                inv2trf = trfact * inv * inv;
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

                if (!beAntiAliasing) {
                    if (j >= 0.f && j < nt - 1) {
                        image[k + (t - prev) * ont] += INTSMP(trace, j, nt, traceposition);
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
                if (nf > maxtri) {
                    nf = (float) maxtri;
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
                    image[k + (t - prev) * ont] += scale * smp;
                }
            }
        }
        // Loop over tau indices

    }


    private void quick_sort(int[] s, int l, int r) {
        if (l < r) {
            Swap(s, l, l + (r - l + 1) / 2);
            int i = l, j = r, x = s[l];
            while (i < j) {
                while (i < j && s[j] >= x) {
                    j--;
                }
                if (i < j)
                    s[i++] = s[j];
                while (i < j && s[i] < x) {
                    i++;
                }
                if (i < j)
                    s[j--] = s[i];
            }
            s[i] = x;
            quick_sort(s, l, i - 1);
            quick_sort(s, i + 1, r);
        }
    }

    private void Swap(int[] s, int oldindex, int newindex) {
        int temp = s[oldindex];
        s[oldindex] = s[newindex];
        s[newindex] = temp;
    }

    private void readPerVelocityData(float[] velocity, int[] ap, int ont, int l, String filename, FileSystem fs, int SIZE) {
        try {
            FSDataInputStream fileIn = fs.open(new Path(filename));
            byte[] temp = new byte[ont * SIZE];
            //byte[] tt = new byte[SIZE];
            for (int i = 0; i < l; i++) {
                fileIn.read(ap[i] * ont * SIZE, temp, 0, temp.length);
                for (int j = 0; j < ont; j++) {

//                    for (int t = 0; t < SIZE; t++) {
//                        tt[t] = temp[SIZE * j + t];
//                    }
                    velocity[i * ont + j] = Float.intBitsToFloat(getInt(temp, j * SIZE));
                }
            }
            fileIn.close();
            temp = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void readData(String path, byte[] datafully, int length, int start, int totalLength, int SIZE,
                          float[] datas, FileSystem fs) {
        try {
            // FileSystem fs = FileSystem.newInstance(conf);
            FSDataInputStream fileIn = fs.open(new Path(path));
            //fileIn.seek(length * start * SIZE);
            //byte[] temp = new byte[4];
            // String result = "";
            //System.out.println("datalength:" + datas.length);
            //byte[] datafully = new byte[totalLength * 4];
            fileIn.read(length * start * SIZE, datafully, 0, SIZE * totalLength);
            //fileIn.read(length * start * SIZE, datafully, 0, datafully.length);
            fileIn.close();
            fileIn = null;
            for (int i = 0; i < totalLength; i++) {
//                for (int j = 0; j < temp.length; j++) {
//                    temp[j] = datafully[4 * i + j];
//                }
                // fileIn.read(temp);
                datas[i] = Float.intBitsToFloat(getInt(datafully, i * 4));
                // result += datas[i] + ",";
            }
            // System.out.println(result);
            // fs.close();
            datafully = null;
            //temp = null;
            //fs = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void readData(String path, int length, int start, int totalLength, int SIZE,
                          float[] datas, FileSystem fs) {
        try {
            // FileSystem fs = FileSystem.newInstance(conf);
            FSDataInputStream fileIn = fs.open(new Path(path));
            //fileIn.seek(length * start * SIZE);
            //byte[] temp = new byte[4];
            // String result = "";
            //System.out.println("datalength:" + datas.length);
            byte[] datafully = new byte[totalLength * 4];
            fileIn.read(length * start * SIZE, datafully, 0, datafully.length);
            //fileIn.read(length * start * SIZE, datafully, 0, datafully.length);
            fileIn.close();
            fileIn = null;
            for (int i = 0; i < totalLength; i++) {
//                for (int j = 0; j < temp.length; j++) {
//                    temp[j] = datafully[4 * i + j];
//                }
                // fileIn.read(temp);
                datas[i] = Float.intBitsToFloat(getInt(datafully, i * 4));
                // result += datas[i] + ",";
            }
            // System.out.println(result);
            // fs.close();
            datafully = null;
            //temp = null;
            //fs = null;
        } catch (Exception e) {
            System.out.println("Exception");
            e.printStackTrace();
        }
    }

    public int getInt(byte[] bytes, int offset) {
        return (0xff & bytes[0 + offset]) | (0xff00 & (bytes[1 + offset] << 8))
                | (0xff0000 & (bytes[2 + offset] << 16))
                | (0xff000000 & (bytes[3 + offset] << 24));
    }


    private void ktMigKernel(float[] trace, int traceindex, float[] vrms,
                             int vrmsindex, float[] image, float ox, float oy, float sx,
                             float sy, float gx, float gy) {

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
            inv2trf = trfact * inv * inv;
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

            if (!beAntiAliasing) {
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
            if (nf > maxtri) {
                nf = (float) maxtri;
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
