package chainhadoop;

import hadoop.CPUKTMigration;
import hadoop.MyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ericson on 2015/1/28 0028.
 */
public class KirchhoffMain {

    private String outputDir = "/";
    private String shotsFile = outputDir + "shot.meta";
    private String cpuSxyFile = outputDir + "fsxy.meta";
    private String cpuGxyFile = outputDir + "fgxy.meta";
    private String cpuCxyFile = outputDir + "fcxy.meta";
    private String rmsvFile = outputDir + "rmsv.meta";
    private String imageFile = "cpuktmig500.data";

    public static void main(String[] args) {
        try {

            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length < 2) {
                System.out
                        .println("Usage:hadoop jar kirchhoff.jar output_path/ filename");
                System.exit(0);
            }
            long start = System.currentTimeMillis();
            KirchhoffMain kirchhoff = new KirchhoffMain();
            kirchhoff.shotsFile = otherArgs[0] + kirchhoff.shotsFile;
            kirchhoff.cpuSxyFile = otherArgs[0] + kirchhoff.cpuSxyFile;
            kirchhoff.cpuGxyFile = otherArgs[0] + kirchhoff.cpuGxyFile;
            kirchhoff.cpuCxyFile = otherArgs[0] + kirchhoff.cpuCxyFile;
            kirchhoff.rmsvFile = otherArgs[0] + kirchhoff.rmsvFile;

            // set the configuration
            conf.set("mapreduce.task.timeout", "1000000000");
            conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.5");
            //conf.set("yarn.nodemanager.resource.memory-mb", "20480");
            conf.set("yarn.scheduler.minimum-allocation-mb", "2048");
            //conf.set("yarn.scheduler.maximum-allocation-mb", "3072");
            conf.set("mapreduce.map.memory.mb", "2048");
            conf.set("mapreduce.reduce.memory.mb", "2048");
            // conf.set("mapreduce.map.java.opts", "-Xmx3072m -Xms1024m");
            // conf.set("mapreduce.reduce.java.opts", "-Xmx2048 -Xms1024m");
            //conf.setInt("mapreduce.task.io.sort.mb", 200);
            //conf.setBoolean("mapreduce.map.output.compress", true);
            conf.set("mapreduce.job.jvm.numtasks", "3");


            hadoop.CPUKTMigration ktm = new CPUKTMigration();
            ktm.setInputFile(kirchhoff.shotsFile, kirchhoff.cpuSxyFile,
                    kirchhoff.cpuGxyFile, kirchhoff.cpuCxyFile,
                    kirchhoff.rmsvFile);
            ktm.setOutputFile(kirchhoff.imageFile);
            ktm.setVerb(true);
            ktm.setAntiAlising(true);
            ktm.ktMigration(1, conf);

            conf.setLong("FileSplitLength", 8 * 1000L);//文件逻辑切分字节
            conf.setInt("SplitPerMap", 1);//一个map中多少个键值对
            conf.set("path", otherArgs[0]);
            //conf.setInt("apx", 1);
            //conf.setInt("apy", 1);

            //printConfPreserve(conf);

            Job job = Job.getInstance(conf);
            job.setJarByClass(KirchhoffMain.class);
            job.setJobName("Kirchhoff");
            job.setInputFormatClass(MyInputFormat.class);


            ChainMapper.addMapper(job, Mapper1.class, IntWritable.class, Text.class, IntWritable.class, Text.class, conf);//求出所有输出道，拼接输入道
            job.setNumReduceTasks(10);
            job.setPartitionerClass(ChainPartitioner.class);
            ChainReducer.setReducer(job, ShotReducer.class, IntWritable.class, Text.class, IntWritable.class, Text.class, conf);//对不同机器上的输出道进行聚合
            ChainReducer.addMapper(job, ThreadMap.class, IntWritable.class, Text.class, IntWritable.class, Text.class, conf);
            ChainReducer.addMapper(job, WriteMap.class, IntWritable.class, Text.class, Text.class, Text.class, conf);


            job.setOutputFormatClass(TextOutputFormat.class);
            FileSystem fs = FileSystem.get(conf);
            if (!fs.exists(new Path(otherArgs[0] + "data/fcxy.data"))) {
                job.killJob();
            }


            FileInputFormat.addInputPath(job, new Path(otherArgs[0] + "data/fcxy.data"));
            Path outputPath = new Path(otherArgs[1]);

            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);

            job.waitForCompletion(true);

            Runtime runtime = Runtime.getRuntime();
            runtime.exec("hadoop dfs -rm -r " + otherArgs[0] + "/map");
            runtime.exec("hadoop dfs -rm -r " + outputPath.toString() + "/part-r*");

            long end = System.currentTimeMillis();
            System.out.println("Times:" + (end - start) + "ms");

            FileStatus[] status = fs.listStatus(outputPath);
            Path imagePath = new Path(outputPath.toString() + "/" + kirchhoff.imageFile);
            List<Integer> lists = new ArrayList<Integer>();
            List<Path> paths = new ArrayList<Path>();
            int countFiles = 0;
            for (FileStatus ss : status) {

                countFiles++;
                String filename = ss.getPath().toString();
                try {
                    //System.out.println("filename:" + filename);
                    filename = filename.substring(filename.lastIndexOf("@") + 1).trim();
                    System.out.println("filename:" + filename);
                    lists.add(Integer.parseInt(filename));
                    paths.add(ss.getPath());
                } catch (Exception e) {

                }
            }

            System.out.println("lists.length:" + lists.size() + ",paths.length:" + paths.size());

            kirchhoff.quick_sort(lists, 0, lists.size() - 1, paths);
            FSDataOutputStream fsOut = fs.create(imagePath, true);
            int cc = 0;
            byte[] bytes = new byte[2000];
            for (int i = 0; i < paths.size(); i++) {
                FSDataInputStream fsIn = fs.open(paths.get(i));
//                fsIn.skip(4);
                while ((cc = fsIn.read(bytes)) != -1) {
                    fsOut.write(bytes, 0, cc);
                }
                fsIn.close();
                fs.delete(paths.get(i), true);
            }
            fsOut.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void quick_sort(List<Integer> s, int l, int r, List<Path> offsetLists) {
        if (l < r) {
            Swap(s, l, l + (r - l + 1) / 2);
            Swap(offsetLists, l, l + (r - l + 1) / 2);
            int i = l, j = r, x = s.get(l);
            while (i < j) {
                while (i < j && s.get(j) >= x) {
                    j--;
                }
                if (i < j)
                    s.set(i++, s.get(j));
                while (i < j && s.get(i) < x) {
                    i++;
                }
                if (i < j)
                    s.set(j--, s.get(i));
            }
            s.set(i, x);
            quick_sort(s, l, i - 1, offsetLists);
            quick_sort(s, i + 1, r, offsetLists);
        }
    }

    private <T> void Swap(List<T> s, int oldindex, int newindex) {
        T temp = s.get(oldindex);
        s.set(oldindex, s.get(newindex));
        s.set(newindex, temp);
    }

    public int getInt(byte[] bytes, int offset) {
        return (0xff & bytes[0 + offset]) | (0xff00 & (bytes[1 + offset] << 8))
                | (0xff0000 & (bytes[2 + offset] << 16))
                | (0xff000000 & (bytes[3 + offset] << 24));
    }

    private static void printConfPreserve(Configuration conf) {
        System.out.println("apx:" + conf.get("apx"));
        System.out.println("apy:" + conf.get("apy"));
        System.out.println("onx:" + conf.get("onx"));
        System.out.println("ony:" + conf.get("ony"));
        System.out.println("oox:" + conf.get("oox"));
        System.out.println("odx:" + conf.get("odx"));
        System.out.println("ooy:" + conf.get("ooy"));
        System.out.println("ody:" + conf.get("ody"));
        System.out.println("nt:" + conf.get("nt"));
        System.out.println("ont:" + conf.get("ont"));
        System.out.println("ot:" + conf.get("ot"));
        System.out.println("dt:" + conf.get("dt"));
        System.out.println("oot:" + conf.get("oot"));
        System.out.println("odt:" + conf.get("odt"));
        System.out.println("maxtri:" + conf.get("maxtri"));
        System.out.println("trfact:" + conf.get("trfact"));
        System.out.println("beDiff:" + conf.get("beDiff"));
        System.out.println("beVerb:" + conf.get("beVerb"));
        System.out.println("beAntiAliasing:" + conf.get("beAntiAliasing"));
        System.out.println("oox:" + conf.get("oox"));
        System.out.println("ooy:" + conf.get("ooy"));
    }

    public byte[] getBytes(int data) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (data & 0xff);
        bytes[1] = (byte) ((data & 0xff00) >> 8);
        bytes[2] = (byte) ((data & 0xff0000) >> 16);
        bytes[3] = (byte) ((data & 0xff000000) >> 24);
        return bytes;
    }
}
