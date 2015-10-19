package spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;

public class MyInputFormat extends FileInputFormat<Integer, String> {

    private long Length = 0;
    private int splitPerMap = 0;

    @Override
    public RecordReader<Integer, String> createRecordReader(InputSplit split,
                                                            TaskAttemptContext context) throws IOException,
            InterruptedException {
        //System.out.println("createRecordReader");
        return new ZRecordReader();
    }

    @Override
    protected long getFormatMinSplitSize() {
        // TODO Auto-generated method stub
        //System.out.println("getFormatMinSplitSize");
        return super.getFormatMinSplitSize();
    }


    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        // TODO Auto-generated method stub
        Configuration conf = job.getConfiguration();
        Length = conf.getLong("FileSplitLength", 0);
        splitPerMap = conf.getInt("SplitPerMap", 1);
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", Length);

        List<InputSplit> originalSplits = super.getSplits(job);
        //System.out.println("originSplit:" + originalSplits.size());
        String[] servers = getActiveServersList(job);
//        for (int i = 0; i < servers.length; i++) {
//            //System.out.println("server:" + servers[i]);
//        }
        if (servers == null)
            return null;
        List<InputSplit> splits = new ArrayList<InputSplit>(originalSplits.size());
        int numSplits = originalSplits.size();
        //System.out.println("numSplits:" + numSplits);
        int currentServer = 0;
        for (int i = 0; i < numSplits; i++, currentServer = getNextServer(currentServer, servers.length)) {
            //System.out.println("numSplits:" + numSplits);
            String server = servers[currentServer];
            //System.out.println("server:" + server);
            boolean replaced = false;
            for (InputSplit split : originalSplits) {
                FileSplit fs = (FileSplit) split;
                //System.out.println("path:" + fs.getPath().toString());
                String[] locations = fs.getLocations();
                //System.out.println("locations.length:" + locations.length);
                for (String l : locations) {
                    //System.out.println("location:" + l);
                    if (l.equals(server)) {
                        //System.out.println("l equals server");
                        splits.add(new FileSplit(fs.getPath(), fs.getStart(), fs.getLength(), new String[]{server}));
                        originalSplits.remove(split);
                        replaced = true;
                        break;
                    }
                }
                if (replaced) {
                    //System.out.println("replaced");
                    break;
                }
            }
            if (!replaced) {
                FileSplit fs = (FileSplit) originalSplits.get(0);
                splits.add(new FileSplit(fs.getPath(), fs.getStart(), fs.getLength(), new String[]{server}));
                originalSplits.remove(0);
            }
        }
        List<InputSplit> lists = new ArrayList<InputSplit>();
//        int index = conf.getInt("INDEX", 0);
//        System.out.println("index:" + index);
        for(int i=0;i<2;i++){
            lists.add(splits.get(i));
        }
        return lists;
//        return splits;
        //System.out.println("getSplits");
        //FileInputFormat.setMaxInputSplitSize((Job) context, 125*8000L);
        //return super.getSplits(context);
    }

    private String[] getActiveServersList(JobContext context) {

        String[] servers = null;
        try {
            JobClient jc = new JobClient((JobConf) context.getConfiguration());
            ClusterStatus status = jc.getClusterStatus(true);
            Collection<String> atc = status.getActiveTrackerNames();
            servers = new String[atc.size()];
            int s = 0;
            for (String serverInfo : atc) {
                //System.out.println("serverInfo:" + serverInfo);
                StringTokenizer st = new StringTokenizer(serverInfo, ":");
                String trackerName = st.nextToken();
                //System.out.println("trackerName:" + trackerName);
                StringTokenizer st1 = new StringTokenizer(trackerName, "_");
                st1.nextToken();
                servers[s++] = st1.nextToken();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return servers;
    }

    private static int getNextServer(int current, int max) {
        //System.out.println("current:" + current);
        current++;
        if (current >= max)
            current = 0;
        return current;
    }

    // 自定义的数据类型
    public static class ZRecordReader extends RecordReader<Integer, String> {
        // data
        // private LineReader in; // 输入流
        private boolean more = true;// 提示后续还有没有数据

        private Integer key = null;
        private String value = "";
        private static long LENGTH;
        private static long SPILL;

        // 这三个保存当前读取到位置（即文件中的位置）
        private long start;
        private long end;
        private long pos;
        private FSDataInputStream fileIn = null;
        private int shotNum = 0;

        public ZRecordReader() {

        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            // 初始化函数
            // System.out.println("initialize");
            // Logger.getLogger("KirchhoffMigration").log(Level.INFO,
            // "enter initialize()");
            FileSplit inputsplit = (FileSplit) split;
            Configuration conf = context.getConfiguration();
            LENGTH = conf.getLong("FileSplitLength", 0);
            SPILL = LENGTH / conf.getInt("SplitPerMap", 1);
//            LENGTH = 32;
//            SPILL = 32;
            assert (LENGTH >= SPILL);
            //System.out.println("length:" + LENGTH);
            //System.out.println("spill:" + SPILL);
            String filename = inputsplit.getPath().toString();
            //System.out.println("filename:" + filename);
//            String buf = filename.substring(filename.lastIndexOf("fcxy") + 4,
//                    filename.lastIndexOf("."));
//            int count = Integer.parseInt(buf);
            // System.out.println(filename);
            // start = inputsplit.getStart(); // 得到此分片开始位置
            start = inputsplit.getStart();
            shotNum += start * 8 / Float.SIZE;
            long offset = LENGTH >= inputsplit.getLength() ? inputsplit
                    .getLength() : LENGTH;
            end = start + offset;// 结束此分片位置
            //System.out.println("inputSplitLength:" + split.getLength());
            //System.out.println("end:" + end);
            // start = inputsplit.getStart(); // 得到此分片开始位置
            // end = start + inputsplit.getLength();// 结束此分片位置
            // System.out.println("start:" + start + " ,end:" + end);
            final Path file = inputsplit.getPath();
            // System.out.println(file.toString());
            // 打开文件
            FileSystem fs = file.getFileSystem(context.getConfiguration());
            fileIn = fs.open(inputsplit.getPath());

            // 关键位置2
            // 将文件指针移动到当前分片，因为每次默认打开文件时，`其指针指向开头
            fileIn.seek(start);

            // in = new LineReader(fileIn, context.getConfiguration());

            // if (start != 0) {
            // System.out.println("not the first split");
            // // 关键解决位置1
            // //
            // 如果这不是第一个分片，那么假设第一个分片是0——4，那么，第4个位置已经被读取，则需要跳过4，否则会产生读入错误，因为你回头又去读之前读过的地方
            // start += (end - pos + 1);
            // }
            pos = start;
        }

        // private int maxBytesToConsume(long pos) {
        // return (int) Math.min(Integer.MAX_VALUE, end - pos);
        // }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            // 下一组值
            // tips:以后在这种函数中最好不要有输出，费时
            // LOG.info("正在读取下一个，嘿嘿");
            if (pos >= end) {
                more = false;
                // System.out.println("pos>=end");
                return false;
            }
            //System.out.println("nextKeyValue");
            if (null == key) {
                key = 0;
            }
            if (null == value) {
                value = "";
            }
            // Text nowline = new Text();// 保存当前行的内容
            // int readsize = in.readLine(nowline);
            byte[] temp = new byte[Float.SIZE / 8];
            long length = (end - start) > SPILL ? SPILL : (end - start);
            byte[] datas = new byte[(int) length];
            float[] data = new float[(int) length * 8 / Float.SIZE];
            int count = 0;
            int nnn = fileIn.read(datas);
            for (int i = 0; i < datas.length * 8 / Float.SIZE; i++) {
                //fileIn.read(temp);
                for (int j = 0; j < temp.length; j++) {
                    temp[j] = datas[4 * i + j];
                }
                data[i] = Float.intBitsToFloat(getInt(temp));
                count++;
            }
            key = shotNum / 2;
            shotNum += data.length;
            String temp_value = "";
            for (int i = 0; i < count; i++) {
                temp_value += String.valueOf(data[i]) + ",";
            }
            temp_value = temp_value.substring(0, temp_value.lastIndexOf(","));
            // temp_value = String.valueOf(count);
            value = temp_value;
            // 更新当前读取到位置
            pos += data.length * temp.length;
//            System.out.println("myinput");
//            System.out.println("key:" + key + ",value:" + data[0]);
            return true;
        }

        public static int getInt(byte[] bytes) {
            return (0xff & bytes[0]) | (0xff00 & (bytes[1] << 8))
                    | (0xff0000 & (bytes[2] << 16))
                    | (0xff000000 & (bytes[3] << 24));
        }

        @Override
        public Integer getCurrentKey() throws IOException,
                InterruptedException {
            // 得到当前的Key
            return key;
        }

        @Override
        public String getCurrentValue() throws IOException, InterruptedException {
            // 得到当前的value
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            // 计算对于当前片的处理进度
            if (false == more || end == start) {
                return 0f;
            } else {
                return Math.min(1.0f, (pos - start) / (end - start));
            }
        }

        @Override
        public void close() throws IOException {
            // 关闭此输入流
            if (null != fileIn) {
                fileIn.close();
            }
        }

    }
}
