package hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MyReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        //System.out.println("Reducer");
        Iterator<Text> ites = values.iterator();
        String connect = ites.next().toString();
        String[] temps = connect.split(",");
        float[] datas = new float[temps.length];
        for (int i = 0; i < temps.length; i++) {
            datas[i] = Float.parseFloat(temps[i]);
        }

        while (ites.hasNext()) {
            connect = ites.next().toString();
            temps = connect.split(",");
            for (int i = 0; i < temps.length; i++) {
                datas[i] += Float.parseFloat(temps[i]);
            }
        }
        String result = "";
        for (int i = 0; i < datas.length; i++) {
            result += String.valueOf(datas[i]) + ",";
        }
        result = result.substring(0, result.lastIndexOf(","));
        context.write(key, new Text(result));
        System.out.println("key:" + key.toString() + ",value:" + datas[0] + "," + datas[datas.length / 2] + "," + datas[datas.length - 1]);
    }
}
