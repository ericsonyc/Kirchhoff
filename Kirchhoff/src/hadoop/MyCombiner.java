package hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MyCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        //System.out.println("Combiner");
        Iterator<Text> ites = values.iterator();
        String connect = ites.next().toString();
        String[] temps = connect.split(",");
        float[] datas = new float[temps.length];
        for (int i = 0; i < temps.length; i++) {
            datas[i] = Float.parseFloat(temps[i]);
        }
        //int count=0;
        while (ites.hasNext()) {
            connect = ites.next().toString();
            temps = connect.split(",");
            for (int i = 0; i < temps.length; i++) {
                datas[i] += Float.parseFloat(temps[i]);
            }
            //count++;
        }
        String result = "";
        for (int i = 0; i < datas.length; i++) {
            result += String.valueOf(datas[i]) + ",";
        }
        result = result.substring(0, result.lastIndexOf(","));
        //System.out.println("count:"+count);
        context.write(key, new Text(result));
    }
}
