package sparknew;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class KirchhoffRead {

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
    private ProgramConf pconf = null;

    public KirchhoffRead(ProgramConf pconf) {
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
        this.pconf = pconf;
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

    public void readData(JavaSparkContext context) {
        KTFile dataFile = new KTFile(context);
        KTFile sxyFile = new KTFile(context);
        KTFile gxyFile = new KTFile(context);
        KTFile cxyFile = new KTFile(context);
        KTFile vrmsFile = new KTFile(context);
        KTFile imageFile = new KTFile(context);
        dataFile.setFilePath(shotsFile);
        sxyFile.setFilePath(sourceCoordinatesFile);
        gxyFile.setFilePath(receiverCoordinatesFile);
        cxyFile.setFilePath(midpointCoordinatesFile);
        vrmsFile.setFilePath(velocityFile);
        int nt, nx, ny, nin, nix, ntr;
        nt = this.histInt(shotsFile, "n1", context);
        nx = this.histInt(shotsFile, "n2", context);
        ny = this.histInt(shotsFile, "n3", context);
        nin = this.histInt(shotsFile, "n4", context);
        nix = this.histInt(shotsFile, "n5", context);
        ntr = nx * ny * nin * nix;

        float dt, ot;
        float temp = this.histfloat(shotsFile, "d1", context);
        dt = (temp == -1.0f) ? 0.0f : temp;
        temp = this.histfloat(shotsFile, "o1", context);
        ot = (temp == -1.0f) ? 0.0f : temp;

        int ont, onx, ony, osize;
        ont = this.histInt(velocityFile, "n1", context);
        onx = this.histInt(velocityFile, "n2", context);
        ony = this.histInt(velocityFile, "n3", context);
        osize = ont * onx * ony;

        float odt, odx, ody, oot, oox, ooy;
        temp = this.histfloat(velocityFile, "d1", context);
        odt = (temp == -1.0f) ? 0.0f : temp;
        temp = this.histfloat(velocityFile, "d2", context);
        odx = (temp == -1.0f) ? 0.0f : temp;
        temp = this.histfloat(velocityFile, "d3", context);
        ody = (temp == -1.0f) ? 1.0f : temp;
        temp = this.histfloat(velocityFile, "o1", context);
        oot = (temp == -1.0f) ? 0.0f : temp;
        temp = this.histfloat(velocityFile, "o2", context);
        oox = (temp == -1.0f) ? 0.0f : temp;
        temp = this.histfloat(velocityFile, "o3", context);
        ooy = (temp == -1.0f) ? 0.0f : temp;

        int n;
        n = this.histInt(sourceCoordinatesFile, "n2", context);

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
        System.out.println("cpuktmigration:apx:" + apx + " ,apy:" + apy);

        //ProgramConf pconf = new ProgramConf(conf);
        pconf.setApx(apx);
        pconf.setApy(apy);
        pconf.setOnx(onx);
        pconf.setOny(ony);
        pconf.setOox(oox);
        pconf.setOdx(odx);
        pconf.setOoy(ooy);
        pconf.setOdy(ody);
        pconf.setNt(nt);
        pconf.setOnt(ont);
        pconf.setOt(ot);
        pconf.setDt(dt);
        pconf.setOot(oot);
        pconf.setOdt(odt);
        pconf.setMaxtri(maxtri);
        pconf.setTrfact(trfact);
        pconf.setBeVerb(beVerb);
        pconf.setBeDiff(beDiff);
        pconf.setBeAntiAliasing(beAntiAliasing);
        pconf.setOox(oox);
        pconf.setOoy(ooy);
        // map parameters
        // conf.set("apx", String.valueOf(apx));
        // conf.set("apy", String.valueOf(apy));
        // conf.set("onx", String.valueOf(onx));
        // conf.set("ony", String.valueOf(ony));
        // conf.set("oox", String.valueOf(oox));
        // conf.set("odx", String.valueOf(odx));
        // conf.set("ooy", String.valueOf(ooy));
        // conf.set("ody", String.valueOf(ody));
        // // reduce parameters
        // conf.set("nt", String.valueOf(nt));
        // conf.set("ont", String.valueOf(ont));
        // conf.set("ot", String.valueOf(ot));
        // conf.set("dt", String.valueOf(dt));
        // conf.set("oot", String.valueOf(oot));
        // conf.set("odt", String.valueOf(odt));
        // conf.set("maxtri", String.valueOf(maxtri));
        // conf.set("trfact", String.valueOf(trfact));
        // conf.set("beVerb", String.valueOf(beVerb));
        // conf.set("beDiff", String.valueOf(beDiff));
        // conf.set("beAntiAliasing", String.valueOf(beAntiAliasing));
        // conf.set("oox", String.valueOf(oox));
        // conf.set("ooy", String.valueOf(ooy));
    }

    public int histInt(String file, String tag, JavaSparkContext context) {
        int value = 1;
        try {
            // FileSystem fs = FileSystem.get(conf);
            // FSDataInputStream fsIn = fs.open(new Path(file));
            // BufferedReader br = new BufferedReader(new
            // InputStreamReader(fsIn));
            // String line = "";
            // while ((line = br.readLine()) != null) {
            // int tagPos = line.indexOf(tag + "=");
            // if (tagPos != -1) {
            // String subStr = line.substring(tagPos + tag.length());
            // int firstCommaIndex = subStr.indexOf(',');
            // int firstEqualIndex = subStr.indexOf('=');
            // value = Integer.parseInt(subStr.substring(
            // firstEqualIndex + 1, firstCommaIndex).trim());
            // break;
            // }
            // }
            // br.close();

            JavaRDD<String> rdd = context.textFile(file, 2);
            List<String> list = rdd.collect();
            for (int i = 0; i < list.size(); i++) {
                String line = list.get(i);
                int tagPos = line.indexOf(tag + "=");
                if (tagPos != -1) {
                    String subStr = line.substring(tagPos + tag.length());
                    int firstCommaIndex = subStr.indexOf(",");
                    int firstEqualIndex = subStr.indexOf("=");
                    value = Integer.parseInt(subStr.substring(
                            firstEqualIndex + 1, firstCommaIndex).trim());
                    break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }

    public float histfloat(String file, String tag, JavaSparkContext context) {
        float value = -1.0f;
        try {
            // FileSystem fs = FileSystem.get(conf);
            // FSDataInputStream fsIn = fs.open(new Path(file));
            // BufferedReader br = new BufferedReader(new
            // InputStreamReader(fsIn));
            // String line = "";
            // while ((line = br.readLine()) != null) {
            // int tagPos = line.indexOf(tag + "=");
            // if (tagPos != -1) {
            // String subStr = line.substring(tagPos + tag.length());
            // int firstCommaIndex = subStr.indexOf(",");
            // int firstEqualIndex = subStr.indexOf("=");
            // value = Float.parseFloat(subStr.substring(
            // firstEqualIndex + 1, firstCommaIndex).trim());
            // break;
            // }
            // }
            // br.close();

            JavaRDD<String> rdd = context.textFile(file, 2);
            List<String> list = rdd.collect();
            for (int i = 0; i < list.size(); i++) {
                String line = list.get(i);
                int tagPos = line.indexOf(tag + "=");
                if (tagPos != -1) {
                    String subStr = line.substring(tagPos + tag.length());
                    int firstCommaIndex = subStr.indexOf(",");
                    int firstEqualIndex = subStr.indexOf("=");
                    value = Float.parseFloat(subStr.substring(
                            firstEqualIndex + 1, firstCommaIndex).trim());
                    break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }
}
