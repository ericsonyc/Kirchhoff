package chainhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ericson on 2015/1/25 0025.
 */
public class ThreadMap extends Mapper<IntWritable, Text, IntWritable, Text> {
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
    private String PATH;

    /**
     * Called once at the beginning of the task.
     *
     * @param context
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("threadmap setup");
        Configuration conf = context.getConfiguration();
        PATH = conf.get("path");
        apx = conf.getInt("apx", 0);
        apy = conf.getInt("apy", 0);
        beVerb = conf.getBoolean("beVerb", true);
        onx = conf.getInt("onx", 0);
        ony = conf.getInt("ony", 0);
        ont = conf.getInt("ont", 0);
        nt = conf.getInt("nt", 0);
        oot = conf.getFloat("oot", 0.0f);
        odt = conf.getFloat("odt", 0.0f);
        dt = conf.getFloat("dt", 0.0f);
        oox = conf.getFloat("oox", 0.0f);
        odx = conf.getFloat("odx", 0.0f);
        ooy = conf.getFloat("ooy", 0.0f);
        ody = conf.getFloat("ody", 0.0f);
        ot = conf.getFloat("ot", 0.0f);
        beDiff = conf.getBoolean("beDiff", true);
        beAntiAliasing = conf.getBoolean("beAntiAliasing", true);
        maxtri = conf.getInt("maxtri", 0);
        trfact = conf.getFloat("trfact", 0.0f);
        System.out.println("apx:" + apx + ",apy:" + apy);
        System.out.println("threadmap setup over");
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
        System.out.println("threadmap run");
        setup(context);
        try {
            while (context.nextKeyValue()) {
                map(context.getCurrentKey(), context.getCurrentValue(), context);
            }
        } finally {
            cleanup(context);
        }
    }

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
        int intKey = key.get();
        System.out.println("map Key:" + intKey);
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        //read velocity
        byte[] datafully = new byte[ont * Float.SIZE / 8];
        float[] velocity = new float[ont];
        this.readData(PATH + "/rmsv.data", datafully, ont, key.get(), velocity.length, Float.SIZE / 8, velocity, fs, false);
        datafully = null;
        System.out.println("velocity:" + velocity[0] + "," + velocity[ont / 2]);

        //get output image
        float[] image = new float[ont];

        //get intput key and sort
        String[] vvs = value.toString().split(",");
        int[] values = new int[vvs.length];
        for (int i = 0; i < values.length; i++) {
            System.out.println("input values:" + vvs[i]);
            values[i] = Integer.parseInt(vvs[i]);
        }
        vvs = null;
        this.quick_sort(values, 0, values.length - 1);

        long startTime = System.currentTimeMillis();
        //split input key with thread
        int place = 4;
        Thread[] threads = new Thread[place];
        int length = values.length / place;
        for (int i = 0; i < place; i++) {
            int[] temp = null;
            if (i == place - 1) {
                temp = new int[values.length - i * length];
            } else {
                temp = new int[length];
            }
            System.out.println("length:" + length);
            for (int j = 0; j < temp.length; j++) {
                temp[j] = values[i * length + j];
            }
            Thread myThread = new MyThread(intKey, velocity, temp, fs, image);
            threads[i] = myThread;
            myThread.start();
        }

        for (int i = 0; i < place; i++) {
            threads[i].join();
        }
        threads = null;
        System.out.println("image:" + image[0] + "," + image[ont / 2]);
        long endTime = System.currentTimeMillis();
        System.out.println("Compute Time:" + (endTime - startTime) + "ms");

        //handle key and value
        String taskId = context.getTaskAttemptID().toString();
        taskId = taskId.substring(taskId.lastIndexOf("r"));
        System.out.println("taskId:" + taskId);
        Path output = new Path(PATH + "/map/" + taskId);
        long offset;
        FSDataOutputStream fsOut = null;
        if (!fs.exists(output)) {
            fsOut = fs.create(output);
            offset = 0;
        } else {
            fsOut = fs.append(output);
            offset = fsOut.getPos();
        }
        System.out.println("offset:" + offset);
        byte[] temp = new byte[image.length * 4];
        for (int i = 0; i < image.length; i++) {
            this.getBytes(Float.floatToIntBits(image[i]), temp, 4 * i);
        }
        fsOut.write(temp);
        fsOut.close();
        String commit = output.toString() + "#" + offset + "#" + temp.length;
        System.out.println("commit:" + commit);
        context.write(key, new Text(commit));
        temp = null;
    }

    public void getBytes(int data, byte[] bytes, int offset) {
        bytes[offset] = (byte) (data & 0xff);
        bytes[1 + offset] = (byte) ((data & 0xff00) >> 8);
        bytes[2 + offset] = (byte) ((data & 0xff0000) >> 16);
        bytes[3 + offset] = (byte) ((data & 0xff000000) >> 24);
    }

    class MyThread extends Thread {
        private int CONSTANT = 1000;
        private int LENGTH;
        private int key;
        private float[] velocity = null;
        private int[] values = null;
        private float[] image = null;
        private FileSystem fs = null;

        public MyThread(int key, float[] velocity, int[] values, FileSystem fs, float[] image) {
            this.key = key;
            this.velocity = velocity;
            this.values = values;
            this.image = image;
            this.fs = fs;
            if (values.length > CONSTANT) {
                LENGTH = CONSTANT;
            } else {
                LENGTH = values.length;
            }
            System.out.println("LENGTH:" + LENGTH);
        }

        @Override
        public void run() {
            byte[] datafully = new byte[Float.SIZE / 8 * CONSTANT * nt];
            float[] trace = new float[CONSTANT * nt];
            this.readData(PATH + "/shot.data", datafully, nt, values[0], LENGTH * nt, Float.SIZE / 8, trace, fs, false);
            float[] sxsy = new float[CONSTANT * 2];
            this.readData(PATH + "/fsxy.data", datafully, 2, values[0], LENGTH * 2, Float.SIZE / 8, sxsy, fs, false);
            float[] gxgy = new float[CONSTANT * 2];
            this.readData(PATH + "/fgxy.data", datafully, 2, values[0], LENGTH * 2, Float.SIZE / 8, gxgy, fs, false);

            System.out.println("trace:" + trace[0] + "," + trace[nt / 2]);
            System.out.println("sxsy:" + sxsy[0] + "," + sxsy[1]);
            System.out.println("gxgy:" + gxgy[0] + "," + gxgy[1]);

            int count = 0;
            float sx, sy, gx, gy;
            int offset;
            float ox, oy;
            for (int i = 0; i < values.length; i++) {
                if (values[i] - values[count] >= LENGTH) {
                    count = i;
                    if (values.length - i > CONSTANT) {
                        LENGTH = CONSTANT;
                    } else {
                        LENGTH = values.length - i;
                    }
                    this.readData(PATH + "/shot.data", datafully, nt, values[i], LENGTH * nt, Float.SIZE / 8, trace, fs, false);
                    this.readData(PATH + "/fsxy.data", datafully, 2, values[i], LENGTH * 2, Float.SIZE / 8, sxsy, fs, false);
                    this.readData(PATH + "/fgxy.data", datafully, 2, values[i], LENGTH * 2, Float.SIZE / 8, gxgy, fs, false);
                }
                offset = values[i] - values[count];
                System.out.println("hhoffset:" + offset);
                sx = sxsy[offset * 2];
                sy = sxsy[offset * 2 + 1];
                gx = gxgy[offset * 2];
                gy = gxgy[offset * 2 + 1];
                System.out.println("sx:" + sx + ",sy:" + sy + ",gx:" + gx + ",gy:" + gy);
                if (beDiff) {
                    this.ktMigSbDiff(trace, nt, offset, dt);
                }
                if (beAntiAliasing) {
                    this.ktMigCint(trace, nt, offset);
                    this.ktMigAcint(trace, nt, offset);
                }
                System.out.println("trace:" + trace[0] + "," + trace[nt / 2]);
                ox = oox + (key % onx) * odx;
                oy = ooy + (key / onx) * ody;
                System.out.println("ox:" + ox + ",oy:" + oy);
                synchronized (image) {
                    this.ktMigKernel(trace, offset, velocity, 0, image, ox, oy, sx, sy, gx, gy, nt, ont, ot, dt, oot, odt, maxtri, trfact, beAntiAliasing);
                }
                System.out.println("image:" + image[0] + "," + image[ont / 2]);
            }
            datafully = null;
            trace = null;
            sxsy = null;
            gxgy = null;
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

        private void readData(String path, byte[] datafully, int length, int start, int totalLength, int SIZE,
                              float[] datas, FileSystem fs, boolean flag) {
            try {
                FSDataInputStream fileIn = fs.open(new Path(path));
                fileIn.read(length * start * SIZE, datafully, 0, SIZE * totalLength);
                fileIn.close();
                fileIn = null;
                for (int i = 0; i < totalLength; i++) {
                    datas[i] = Float.intBitsToFloat(getInt(datafully, i * 4, flag));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }

    private void readData(String path, byte[] datafully, int length, int start, int totalLength, int SIZE,
                          float[] datas, FileSystem fs, boolean flag) {
        try {
            FSDataInputStream fileIn = fs.open(new Path(path));
            fileIn.read(length * start * SIZE, datafully, 0, SIZE * totalLength);
            fileIn.close();
            fileIn = null;
            for (int i = 0; i < totalLength; i++) {
                datas[i] = Float.intBitsToFloat(getInt(datafully, i * 4, flag));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int getInt(byte[] bytes, int offset, boolean flag) {
        if (flag) {
            return (0xff & bytes[3 + offset]) | (0xff00 & (bytes[2 + offset] << 8))
                    | (0xff0000 & (bytes[1 + offset] << 16))
                    | (0xff000000 & (bytes[0 + offset] << 24));
        } else {
            return (0xff & bytes[0 + offset]) | (0xff00 & (bytes[1 + offset] << 8))
                    | (0xff0000 & (bytes[2 + offset] << 16))
                    | (0xff000000 & (bytes[3 + offset] << 24));
        }

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
