package hdfswrite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.nio.charset.Charset;

public class KTFile {

    private String filePath;
    private int currentPos;
    Configuration conf = null;

    public KTFile(Configuration conf) {
        filePath = "";
        currentPos = 0;
        this.conf = conf;
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
            FileSystem fs = FileSystem.get(conf);
            Path filepath = new Path(path);
            FSDataInputStream fsIn = fs.open(filepath);
            byte[] temp = new byte[250];
            fsIn.read(temp);
            String buf = getString(temp);
            buf = buf.substring(buf.indexOf("=") + 1, buf.indexOf("\n")).trim();
            this.filePath = buf;
            fsIn.close();
            //br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }

    public String getString(byte[] bytes, String charsetName) {
        return new String(bytes, Charset.forName(charsetName));
    }

    public String getString(byte[] bytes) {
        return getString(bytes, "GBK");
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
