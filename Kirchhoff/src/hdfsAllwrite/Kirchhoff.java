package hdfsAllwrite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.ArrayList;
import java.util.List;

public class Kirchhoff {

    private String outputDir = "/";
    private String shotsFile = outputDir + "shot.meta";
    private String cpuSxyFile = outputDir + "fsxy.meta";
    private String cpuGxyFile = outputDir + "fgxy.meta";
    private String cpuCxyFile = outputDir + "fcxy.meta";
    private String rmsvFile = outputDir + "rmsv.meta";
    private String imageFile = "cpuktmig500.data";

    public static void main(String[] args) {
        try {
            // Logger.getLogger("KirchhoffMigration").log(Level.INFO,
            // "enter main");
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length < 2) {
                System.out
                        .println("Usage:hadoop jar kirchhoff.jar output_path/ filename");
                System.exit(0);
            }
            long start = System.currentTimeMillis();
            Kirchhoff kirchhoff = new Kirchhoff();
            kirchhoff.shotsFile = otherArgs[0] + kirchhoff.shotsFile;
            kirchhoff.cpuSxyFile = otherArgs[0] + kirchhoff.cpuSxyFile;
            kirchhoff.cpuGxyFile = otherArgs[0] + kirchhoff.cpuGxyFile;
            kirchhoff.cpuCxyFile = otherArgs[0] + kirchhoff.cpuCxyFile;
            kirchhoff.rmsvFile = otherArgs[0] + kirchhoff.rmsvFile;

            conf.setLong("FileSplitLength", 8000L);//文件逻辑切分字节
            conf.setInt("SplitPerMap", 1);//一个map中多少个键值对

            // set the configuration
            conf.set("mapreduce.task.timeout", "1000000000");
            conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.5");
            //conf.set("yarn.nodemanager.resource.memory-mb", "20480");
            conf.set("yarn.scheduler.minimum-allocation-mb", "1024");
            //conf.set("yarn.scheduler.maximum-allocation-mb", "3072");
            conf.set("mapreduce.map.memory.mb", "1024");
            conf.set("mapreduce.reduce.memory.mb", "1024");
            // conf.set("mapreduce.map.java.opts", "-Xmx3072m -Xms1024m");
            // conf.set("mapreduce.reduce.java.opts", "-Xmx2048 -Xms1024m");
            //conf.setInt("mapreduce.task.io.sort.mb", 200);
            //conf.setBoolean("mapreduce.map.output.compress", true);
            conf.set("mapreduce.job.jvm.numtasks", "3");

            conf.set("path", otherArgs[0]);
            //conf.setInt("apx", 20);
            //conf.setInt("apy", 20);
            CPUKTMigration ktm = new CPUKTMigration();
            ktm.setInputFile(kirchhoff.shotsFile, kirchhoff.cpuSxyFile,
                    kirchhoff.cpuGxyFile, kirchhoff.cpuCxyFile,
                    kirchhoff.rmsvFile);
            ktm.setOutputFile(kirchhoff.imageFile);
            ktm.setVerb(true);
            ktm.setAntiAlising(true);
            ktm.ktMigration(1, conf);
//            conf.setInt("apx", 10);
            //printConfPreserve(conf);

            Job job = Job.getInstance(conf);
            job.setJarByClass(Kirchhoff.class);
            job.setJobName("Kirchhoff");
            job.setInputFormatClass(MyInputFormat.class);

//            System.out.println("MultiMap");
            //MultiMap.setNumberOfThreads(job, 2);
            //MultiMap.setMapperClass(job, CPUKTMigrationNoThread.class);
            job.setMapperClass(CPUKTMigrationMapWriteHdfs.class);

            //MultithreadedMapper.setMapperClass(job, CPUKTMigration.class);
            //MultithreadedMapper.setNumberOfThreads(job, 4);
            job.setCombinerClass(ReadFileCombine.class);
            job.setPartitionerClass(ShotPartitioner.class);
            job.setReducerClass(ReadFileReducer.class);
            job.setNumReduceTasks(4);
            FileSystem fs = FileSystem.get(conf);
            if (!fs.exists(new Path(otherArgs[0] + "/fcxy.data"))) {
                job.killJob();
            }
//            long startTime = System.currentTimeMillis();
//            for (int i = 0; i < 10; i++) {
//                String filename = otherArgs[0] + "data/fcxy" + i + ".data";
//                if (!fs.exists(new Path(filename))) {
//                    byte[] bytes = new byte[1392000];
//                    FSDataInputStream fsIn = fs.open(new Path(otherArgs[0]
//                            + "data/fcxy.data"));
//                    fsIn.readFully(0, bytes);
//                    fsIn.close();
//                    FSDataOutputStream fsOut = fs.create(new Path(filename));
//                    fsOut.write(bytes);
//                    fsOut.close();
//                }
//                FileInputFormat.addInputPath(job, new Path(filename));
//            }
//            long endTime = System.currentTimeMillis();
//            System.out.println("Time:" + (endTime - startTime) + "ms");
            FileInputFormat.addInputPath(job, new Path(otherArgs[0] + "/fcxy.data"));
            Path outputPath = new Path(otherArgs[1]);

            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setOutputFormatClass(MyOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setOutputFormatClass(MyOutputFormat.class);
            job.waitForCompletion(true);
            Runtime runtime = Runtime.getRuntime();
            runtime.exec("hadoop dfs -rm -r " + otherArgs[0] + "/map");

            long end = System.currentTimeMillis();
            System.out.println("Times:" + (end - start) + "ms");

            FileStatus[] status = fs.listStatus(outputPath);
            List<Integer> indexes = new ArrayList<Integer>();
            List<Integer> noIndexes = new ArrayList<Integer>();
            List<Integer> datass = new ArrayList<Integer>();
            for (int i = 0; i < status.length; i++) {
                String strtemp = status[i].getPath().toString();
                if (strtemp.lastIndexOf("#") != -1) {
                    datass.add(Integer.parseInt(strtemp.substring(strtemp.lastIndexOf("#") + 1)));
                    indexes.add(i);
                } else {
                    noIndexes.add(i);
                }
            }

            for (int i = 0; i < datass.size(); i++) {
                for (int j = 0; j < datass.size() - i - 1; j++) {
                    if (datass.get(j) > datass.get(j + 1)) {
                        int temp = datass.get(j);
                        datass.set(j, datass.get(j + 1));
                        datass.set(j + 1, temp);
                        temp = indexes.get(j);
                        indexes.set(j, indexes.get(j + 1));
                        indexes.set(j + 1, temp);
                    }
                }
            }

            Path imagePath = new Path(outputPath.toString() + "/" + kirchhoff.imageFile);
            FSDataOutputStream fsOut = fs.create(imagePath, true);
            byte[] bytes = new byte[2000];
            int cc = 0;
            for (int i = 0; i < indexes.size(); i++) {
                FileStatus ss = status[indexes.get(i)];
//                System.out.println(ss.getPath());
                FSDataInputStream fsIn = fs.open(ss.getPath());
                while ((cc = fsIn.read(bytes)) != -1) {
                    fsOut.write(bytes, 0, cc);
                }
                fsIn.close();
                //fs.delete(ss.getPath(), true);
            }
            fsOut.close();

            for (int i = 0; i < noIndexes.size(); i++) {
                FileStatus ss = status[noIndexes.get(i)];
                //fs.delete(ss.getPath(), true);
            }

//            FileStatus[] status = fs.listStatus(outputPath);
//            Path imagePath = new Path(outputPath.toString() + "/" + kirchhoff.imageFile);
//            FSDataOutputStream fsOut = fs.create(imagePath, true);
//            byte[] bytes = new byte[2000];
//            int cc = 0;
//            for (FileStatus ss : status) {
//                FSDataInputStream fsIn = fs.open(ss.getPath());
//                while ((cc = fsIn.read(bytes)) != -1) {
//                    fsOut.write(bytes, 0, cc);
//                }
//                fsIn.close();
//                fs.delete(ss.getPath(), true);
//            }
//            fsOut.close();
            // FileSystem fsout = FileSystem.get(conf);
            /*FileStatus[] status = fs.listStatus(outputPath);
            ArrayList<Integer> keys = new ArrayList<Integer>();
            ArrayList<String> values = new ArrayList<String>();
            for (FileStatus ss : status) {
                FSDataInputStream fsIn = fs.open(ss.getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(
                        fsIn));
                String str = null;
                while ((str = br.readLine()) != null) {
                    String[] datas = str.split("#");
                    keys.add(Integer.parseInt(datas[0]));
                    values.add(datas[1]);
                }
                br.close();
                fsIn.close();
            }

            for (int i = 0; i < keys.size(); i++) {
                for (int j = 0; j < keys.size() - i - 1; j++) {
                    if (keys.get(j) > keys.get(j + 1)) {
                        int temp = keys.get(j);
                        keys.set(j, keys.get(j + 1));
                        keys.set(j + 1, temp);
                        String t = values.get(j);
                        values.set(j, values.get(j + 1));
                        values.set(j + 1, t);
                    }
                }
            }
            // fs.delete(outputPath, true);
            kirchhoff.writeImage(conf, outputPath, values);*/
        } catch (Exception e) {
            e.printStackTrace();
        }

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

    private boolean writeImage(Configuration conf, Path rote,
                               ArrayList<String> values) {
        if (rote == null) {
            return false;
        }
        try {
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(rote)) {
                fs.delete(rote, true);
            }
            Path result = new Path(rote.toString() + "/cpuktmig500.data");
            FSDataOutputStream fsout = fs.create(result);
            for (int i = 0; i < values.size(); i++) {
                String[] temp = values.get(i).split(",");
                for (int j = 0; j < temp.length; j++) {
                    int x = Float.floatToIntBits(Float.parseFloat(temp[j]));
                    fsout.write(getBytes(x));
                }
                fsout.flush();
            }
            fsout.close();

            // System.out.println("the length of the count is:"+count);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
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
