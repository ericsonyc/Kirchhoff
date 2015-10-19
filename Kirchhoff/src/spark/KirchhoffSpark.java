package spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ericson on 2015/3/10 0010.
 */
public class KirchhoffSpark {
    private String outputDir = "/";
    private String shotsFile = outputDir + "shot.meta";
    private String cpuSxyFile = outputDir + "fsxy.meta";
    private String cpuGxyFile = outputDir + "fgxy.meta";
    private String cpuCxyFile = outputDir + "fcxy.meta";
    private String rmsvFile = outputDir + "rmsv.meta";
    private String imageFile = outputDir + "cpuktmig500.data";

    public static void main(String[] args) throws Exception {
        System.setProperty("spark.akka.frameSize", "100");
        System.setProperty("spark.default.parallelism", "15");
        SparkConf conf = new SparkConf().setAppName("KirchhoffSpark");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        Configuration hadoopConf = jsc.hadoopConfiguration();
        String[] otherArgs = new GenericOptionsParser(hadoopConf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.out.println("enter path");
            System.exit(0);
        }

        long start = System.currentTimeMillis();
        KirchhoffSpark spark = new KirchhoffSpark();
        spark.shotsFile = otherArgs[0] + spark.shotsFile;
        spark.cpuSxyFile = otherArgs[0] + spark.cpuSxyFile;
        spark.cpuGxyFile = otherArgs[0] + spark.cpuGxyFile;
        spark.cpuCxyFile = otherArgs[0] + spark.cpuCxyFile;
        spark.rmsvFile = otherArgs[0] + spark.rmsvFile;

        ProgramConf pconf = new ProgramConf(conf);
        KirchhoffRead kirchhoff = new KirchhoffRead(pconf);
        kirchhoff.setInputFile(spark.shotsFile, spark.cpuSxyFile,
                spark.cpuGxyFile, spark.cpuCxyFile, spark.rmsvFile);
        kirchhoff.setOutputFile(spark.imageFile);
        kirchhoff.setVerb(true);
        kirchhoff.setAntiAlising(true);
        kirchhoff.readData(jsc);
        long end = System.currentTimeMillis();
        System.out.println("pre handle time:" + (end - start) + "ms");

        pconf.setPath(otherArgs[0]);

        hadoopConf.setLong("FileSplitLength", 8000L);//文件逻辑切分字节
        hadoopConf.setInt("SplitPerMap", 1);//一个map中多少个键值对

        System.out.println("begin RDD");
        JavaPairRDD<Integer, String> pairRdd = jsc.newAPIHadoopFile(otherArgs[0]
                        + "/fcxy.data", MyInputFormat.class, Integer.class,
                String.class, hadoopConf).repartition(15);
//        List<Tuple2<Integer, String>> tuples = pairRdd.collect();
//        System.out.println("tuples.length:" + tuples.size());
//        for (int i = 0; i < tuples.size(); i++) {
//            Tuple2<Integer, String> tuple = tuples.get(i);
//            String[] temp = tuple._2().split(",");
//            System.out.println("tuple key:" + tuple._1() + ",value:" + temp[0] + "," + temp[1]);
//        }

        System.out.println("begin map pair");
        JavaPairRDD<Integer, String> output = pairRdd.flatMapToPair(new MyPairMapFunction(pconf));
//        JavaPairRDD<Integer, String> rdd = output.partitionBy(new ScalaPartitioner(30, pconf.getOnx())).flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, String>, Integer, String>() {
//            @Override
//            public Iterable<Tuple2<Integer, String>> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
//                return null;
//            }
//        });

        JavaPairRDD<Integer, String> rdd = output.partitionBy(new ScalaPartitioner(25, pconf.getOnx())).reduceByKey(new ReduceFuction());

        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            Path path = new Path(otherArgs[1]);
            System.out.println("path:" + path.toString());
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
            rdd.saveAsNewAPIHadoopFile(path.toString(), Integer.class, String.class, MyOutputFormat.class, hadoopConf);
            System.out.println("saveFile");
            FileStatus[] status = fs.listStatus(path);
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

            Path imagePath = new Path(path.toString() + "/" + spark.imageFile);
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
                fs.delete(ss.getPath(), true);
            }
            fsOut.close();

            for (int i = 0; i < noIndexes.size(); i++) {
                FileStatus ss = status[noIndexes.get(i)];
                fs.delete(ss.getPath(), true);
            }
        } catch (Exception e) {
            System.out.println("Error in Main:" + e.getMessage());
            e.printStackTrace();
        }
    }

}
