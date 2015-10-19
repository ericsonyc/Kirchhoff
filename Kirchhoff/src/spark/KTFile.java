package spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;

import java.util.List;

public class KTFile extends KryoSerializer {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private String filePath;
    private int currentPos;
    private JavaSparkContext context = null;

    public KTFile(JavaSparkContext context) {
        super(context.getConf());
        filePath = "";
        currentPos = 0;
        this.context = context;
    }

    public String getFilePath() {
        return this.filePath;
    }

    public boolean setFilePath(String path) {
//		try {
//			BufferedReader br = new BufferedReader(new FileReader(path));
//			String buf = br.readLine();
//			buf = buf.substring(buf.lastIndexOf("= ") + 1).trim();
//			this.filePath = buf;
//			br.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}

        try {
//			FileSystem fs=FileSystem.get(conf);
//			Path filepath=new Path(path);
//			FSDataInputStream fsIn=fs.open(filepath);
//			BufferedReader br=new BufferedReader(new InputStreamReader(fsIn));
//			String buf=br.readLine();
//			buf=buf.substring(buf.lastIndexOf("= ")+1).trim();
//			this.filePath=buf;
//			br.close();

            JavaRDD<String> rdd = context.textFile(path, 2);
            List<String> list = rdd.collect();
            String buf = list.get(0);
            buf = buf.substring(buf.lastIndexOf("= ") + 1).trim();
            this.filePath = buf;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }

    public int getCurrentPos(int pos) {
        return currentPos;
    }

    public void setCurrentPos(int pos) {
        this.currentPos = pos;
    }

    public void resetFile() {
        this.currentPos = 0;
    }

    public void clearFile() {
        this.currentPos = 0;
    }
}
