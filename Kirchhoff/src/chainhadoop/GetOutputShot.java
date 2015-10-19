package chainhadoop;

import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;

class GetOutputShot {
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
    private HashMap<Integer, String> maps = null;
    private Configuration conf = null;

    public GetOutputShot(int[] keys, float[] fcxy, Configuration conf, HashMap<Integer, String> maps) {
        this.keys = keys;
        this.fcxy = fcxy;
        this.conf = conf;
        this.maps = maps;
    }

    public void runOutputShot() {
        // TODO Auto-generated method stub
        try {
            if (keys.length > 0)
                getOuputShot(keys, fcxy);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void getOuputShot(int[] keys, float[] cxcy) throws Exception {
        // System.out.println("getOutputShot");
        // long start = System.currentTimeMillis();
        apx = conf.getInt("apx", 20);
        apy = conf.getInt("apy", 20);
        System.out.println("apx:" + apx + ",apy:" + apy);
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
        String PATH = conf.get("path");

        int[] ap = new int[onx * ony];

        for (int i = 0; i < keys.length; ++i) {
            int minix = onx - 1;
            int maxix = 0;
            int miniy = ony - 1;
            int maxiy = 0;
            int ix, iy;
            // 计算输入道在输出结果中的位置
            ix = (int) ((cxcy[2 * i] - oox) / odx + 0.5f);// 计算idxi
            iy = (int) ((cxcy[2 * i + 1] - ooy) / ody + 0.5f);// 计算idyi
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

            int l = 0;
            // 计算输出道
            int iiy, iix, oidx;
            for (iiy = miniy; iiy <= maxiy; ++iiy) {
                for (iix = minix; iix <= maxix; ++iix) {
                    oidx = iiy * onx + iix;
                    if ((iix >= el_cx1 && iix <= el_cx2)
                            && (iiy >= el_cy1 && iiy <= el_cy2)) {
                        ap[l] = oidx;
                        ++l;
                        continue;
                    }

                    float el_x = (iix < el_cx1) ? iix - el_cx1 : iix - el_cx2;
                    float el_y = (iiy < el_cy1) ? iiy - el_cy1 : iiy - el_cy2;
                    // Check if the point is within one of the ellipses
                    if ((el_x * el_x / (apx * apx) + (el_y * el_y)
                            / (apy * apy)) < 1.0f) {
                        ap[l] = oidx;
                        ++l;
                    }
                }// for ix
            }// for iy

            this.quick_sort(ap, 0, l - 1);

            for (int j = 0; j < l; j++) {
                synchronized (maps) {
//                    HashMap<Integer, String> map = maps[ap[j]
//                            % maps.length];
                    if (!maps.containsKey(ap[j])) {
                        maps.put(ap[j], String.valueOf(keys[i]));
                    } else {
                        String tt = maps.get(ap[j]);
                        maps.put(ap[j], tt + "," + keys[i]);
                    }
//                    maps[ap[j] % maps.length] = map;
                }
            }

            System.out.println("l:" + l);


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
}
