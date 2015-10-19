package sparknew;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;

public class ProgramConf extends KryoSerializer {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public ProgramConf(SparkConf conf) {
        super(conf);
        // TODO Auto-generated constructor stub
    }

    private int nt, nx, ny, nin, nix, ntr;
    private float dt, ot;
    private int ont, onx, ony, osize;
    private float odt, odx, ody, oot, oox, ooy;
    private int apx;
    private int apy;
    private int maxtri;
    private float trfact;
    private boolean beAntiAliasing;
    private boolean beDiff;
    private boolean beVerb;
    private String path;
    private long FileSplitLength;
    private int SplitPerMap;

    public int getSplitPerMap() {
        return SplitPerMap;
    }

    public void setSplitPerMap(int splitPerMap) {
        SplitPerMap = splitPerMap;
    }

    public long getFileSplitLength() {
        return FileSplitLength;
    }

    public void setFileSplitLength(long fileSplitLength) {
        FileSplitLength = fileSplitLength;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getApx() {
        return apx;
    }

    public void setApx(int apx) {
        this.apx = apx;
    }

    public int getApy() {
        return apy;
    }

    public void setApy(int apy) {
        this.apy = apy;
    }

    public int getMaxtri() {
        return maxtri;
    }

    public void setMaxtri(int maxtri) {
        this.maxtri = maxtri;
    }

    public float getTrfact() {
        return trfact;
    }

    public void setTrfact(float trfact) {
        this.trfact = trfact;
    }

    public boolean isBeAntiAliasing() {
        return beAntiAliasing;
    }

    public void setBeAntiAliasing(boolean beAntiAliasing) {
        this.beAntiAliasing = beAntiAliasing;
    }

    public boolean isBeDiff() {
        return beDiff;
    }

    public void setBeDiff(boolean beDiff) {
        this.beDiff = beDiff;
    }

    public boolean isBeVerb() {
        return beVerb;
    }

    public void setBeVerb(boolean beVerb) {
        this.beVerb = beVerb;
    }

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    public int getNt() {
        return nt;
    }

    public void setNt(int nt) {
        this.nt = nt;
    }

    public int getNx() {
        return nx;
    }

    public void setNx(int nx) {
        this.nx = nx;
    }

    public int getNy() {
        return ny;
    }

    public void setNy(int ny) {
        this.ny = ny;
    }

    public int getNin() {
        return nin;
    }

    public void setNin(int nin) {
        this.nin = nin;
    }

    public int getNix() {
        return nix;
    }

    public void setNix(int nix) {
        this.nix = nix;
    }

    public int getNtr() {
        return ntr;
    }

    public void setNtr(int ntr) {
        this.ntr = ntr;
    }

    public float getDt() {
        return dt;
    }

    public void setDt(float dt) {
        this.dt = dt;
    }

    public float getOt() {
        return ot;
    }

    public void setOt(float ot) {
        this.ot = ot;
    }

    public int getOnt() {
        return ont;
    }

    public void setOnt(int ont) {
        this.ont = ont;
    }

    public int getOnx() {
        return onx;
    }

    public void setOnx(int onx) {
        this.onx = onx;
    }

    public int getOny() {
        return ony;
    }

    public void setOny(int ony) {
        this.ony = ony;
    }

    public int getOsize() {
        return osize;
    }

    public void setOsize(int osize) {
        this.osize = osize;
    }

    public float getOdt() {
        return odt;
    }

    public void setOdt(float odt) {
        this.odt = odt;
    }

    public float getOdx() {
        return odx;
    }

    public void setOdx(float odx) {
        this.odx = odx;
    }

    public float getOdy() {
        return ody;
    }

    public void setOdy(float ody) {
        this.ody = ody;
    }

    public float getOot() {
        return oot;
    }

    public void setOot(float oot) {
        this.oot = oot;
    }

    public float getOox() {
        return oox;
    }

    public void setOox(float oox) {
        this.oox = oox;
    }

    public float getOoy() {
        return ooy;
    }

    public void setOoy(float ooy) {
        this.ooy = ooy;
    }

}
