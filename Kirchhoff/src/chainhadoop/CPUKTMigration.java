package chainhadoop;

import hadoop.KTFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.nio.charset.Charset;

public class CPUKTMigration {

    private boolean beAntiAliasing;
    private boolean beDiff;
    private boolean beVerb;
    private String shotsFile;
    private String sourceCoordinatesFile;
    private String receiverCoordinatesFile;
    private String midpointCoordinatesFile;
    private String velocityFile;
    private String outputFilePath;
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

    // the variables that mapper used
    // int nt, nx, ny, nin, nix, ntr;
    // float dt, ot;
    // int ont, onx, ony, osize;
    // float odt, odx, ody, oot, oox, ooy;

    public CPUKTMigration() {
        beAntiAliasing = true;
        beDiff = true;
        beVerb = false;
        shotsFile = "";
        sourceCoordinatesFile = "";
        receiverCoordinatesFile = "";
        midpointCoordinatesFile = "";
        velocityFile = "";
        outputFilePath = "";
        apx = 0;
        apy = 0;
        maxtri = 0;
        trfact = 0;
    }

    public void setInputFile(String shots, String sxy, String gxy, String cxy,
                             String rmsv) {
        this.shotsFile = shots;
        this.sourceCoordinatesFile = sxy;
        this.receiverCoordinatesFile = gxy;
        this.midpointCoordinatesFile = cxy;
        this.velocityFile = rmsv;
    }

    public void setOutputFile(String imageFile) {
        this.outputFilePath = imageFile;
    }

    public void setVerb(boolean verb) {
        this.beVerb = verb;
    }

    public void setAntiAlising(boolean antialising) {
        this.beAntiAliasing = antialising;
    }

    public String getString(byte[] bytes) {
        return getString(bytes, "GBK");
    }

    public String getString(byte[] bytes, String charsetName) {
        return new String(bytes, Charset.forName(charsetName));
    }

    public int histInt(String file, String tag, Configuration conf) {
        int value = 1;
        try {

            FileSystem fs = FileSystem.get(conf);
            Path filepath = new Path(file);
            FSDataInputStream fsIn = fs.open(filepath);
            byte[] temp = new byte[250];
            fsIn.read(temp);
            String buf = getString(temp);
            //System.out.println("buf:" + buf);
            //System.out.println("tag:" + tag);
            if (buf.indexOf(tag + "=") != -1) {
                buf = buf.substring(buf.indexOf(tag + "=") + tag.length() + 1).trim();
                //System.out.println("middlebuf:" + buf);
                buf = buf.substring(0, buf.indexOf(",")).trim();
                //System.out.println("endbuf:" + buf);
                value = Integer.parseInt(buf);
                //System.out.println("value:" + value);
                fsIn.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }

    public float histfloat(String file, String tag, Configuration conf) {
        float value = -1.0f;
        try {
            FileSystem fs = FileSystem.get(conf);
            Path filepath = new Path(file);
            FSDataInputStream fsIn = fs.open(filepath);
            byte[] temp = new byte[250];
            fsIn.read(temp);
            String buf = getString(temp);
            if (buf.indexOf(tag + "=") != -1) {
                buf = buf.substring(buf.indexOf(tag + "=") + tag.length() + 1).trim();
                buf = buf.substring(0, buf.indexOf(",")).trim();
                value = Float.parseFloat(buf);
                //System.out.println("value:" + value);
                fsIn.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }

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

    public void ktMigration(int dbtr, Configuration conf) {
        KTFile dataFile = new KTFile(conf);
        KTFile sxyFile = new KTFile(conf);
        KTFile gxyFile = new KTFile(conf);
        KTFile cxyFile = new KTFile(conf);
        KTFile vrmsFile = new KTFile(conf);
        KTFile imageFile = new KTFile(conf);
        dataFile.setFilePath(shotsFile);
        sxyFile.setFilePath(sourceCoordinatesFile);
        gxyFile.setFilePath(receiverCoordinatesFile);
        cxyFile.setFilePath(midpointCoordinatesFile);
        vrmsFile.setFilePath(velocityFile);

        int nt, nx, ny, nin, nix, ntr;
        nt = this.histInt(shotsFile, "n1", conf);
        nx = this.histInt(shotsFile, "n2", conf);
        ny = this.histInt(shotsFile, "n3", conf);
        nin = this.histInt(shotsFile, "n4", conf);
        nix = this.histInt(shotsFile, "n5", conf);
        ntr = nx * ny * nin * nix;

        float dt, ot;
        float temp = this.histfloat(shotsFile, "d1", conf);
        dt = (temp == -1.0f) ? 0.0f : temp;
        temp = this.histfloat(shotsFile, "o1", conf);
        ot = (temp == -1.0f) ? 0.0f : temp;

        int ont, onx, ony, osize;
        ont = this.histInt(velocityFile, "n1", conf);
        onx = this.histInt(velocityFile, "n2", conf);
        ony = this.histInt(velocityFile, "n3", conf);
        osize = ont * onx * ony;

        float odt, odx, ody, oot, oox, ooy;
        temp = this.histfloat(velocityFile, "d1", conf);
        odt = (temp == -1.0f) ? 0.0f : temp;
        temp = this.histfloat(velocityFile, "d2", conf);
        odx = (temp == -1.0f) ? 0.0f : temp;
        temp = this.histfloat(velocityFile, "d3", conf);
        ody = (temp == -1.0f) ? 1.0f : temp;
        temp = this.histfloat(velocityFile, "o1", conf);
        oot = (temp == -1.0f) ? 0.0f : temp;
        temp = this.histfloat(velocityFile, "o2", conf);
        oox = (temp == -1.0f) ? 0.0f : temp;
        temp = this.histfloat(velocityFile, "o3", conf);
        ooy = (temp == -1.0f) ? 0.0f : temp;

        int n;
        n = this.histInt(sourceCoordinatesFile, "n2", conf);

        if (this.apx == 0) {
            apx = onx / 2;
        }
        if (this.apy == 0) {
            apy = (ony + 1) / 2;
        }
        if (this.maxtri == 0) {
            this.maxtri = 13;
        }
        if (trfact == 0) {
            this.trfact = 4.0f * (0.5f * (odx + ody) / dt);
        }

        // map parameters
        conf.setInt("apx", apx);
        conf.setInt("apy", apy);
        conf.setInt("onx", onx);
        conf.setInt("ony", ony);
        conf.setFloat("oox", oox);
        conf.setFloat("odx", odx);
        conf.setFloat("ooy", ooy);
        conf.setFloat("ody", ody);
        // reduce parameters
        conf.setInt("nt", nt);
        conf.setInt("ont", ont);
        conf.setFloat("ot", ot);
        conf.setFloat("dt", dt);
        conf.setFloat("oot", oot);
        conf.setFloat("odt", odt);
        conf.setInt("maxtri", maxtri);
        conf.setFloat("trfact", trfact);
        conf.setBoolean("beDiff", beDiff);
        conf.setBoolean("beVerb", beVerb);
        conf.setBoolean("beAntiAliasing", beAntiAliasing);
        conf.setFloat("oox", oox);
        conf.setFloat("ooy", ooy);
    }
}
