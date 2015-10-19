package hadoop;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by ericson on 2015/1/22 0022.
 */
public class ShotPartitioner extends Partitioner<Text, Text> implements Configurable {

    private Configuration configuration = null;

    /**
     * Get the partition number for a given key (hence record) given the total
     * number of partitions i.e. number of reduce-tasks for the job.
     * <p/>
     * <p>Typically a hash function on a all or a subset of the key.</p>
     *
     * @param text          the key to be partioned.
     * @param text2         the entry value.
     * @param numPartitions the total number of partitions.
     * @return the partition number for the <code>key</code>.
     */
    @Override
    public int getPartition(Text text, Text text2, int numPartitions) {
        int onx = configuration.getInt("onx", 1);
        int length = onx / numPartitions;
        int key = Integer.parseInt(text.toString());
        int reduce = key / length;
        if (reduce >= numPartitions) {
            return numPartitions - 1;
        } else {
            return reduce;
        }
    }

    /**
     * Set the configuration to be used by this object.
     *
     * @param conf
     */
    @Override
    public void setConf(Configuration conf) {
        System.out.println("********conf");
        configuration = conf;
    }

    /**
     * Return the configuration used by this object.
     */
    @Override
    public Configuration getConf() {
        System.out.println("************getConf");
        return configuration;
    }
}
