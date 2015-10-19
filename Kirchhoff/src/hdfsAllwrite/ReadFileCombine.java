package hdfsAllwrite;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ReadFileCombine extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
//        System.out.println("Reducer");
//        Iterator<Text> ites = values.iterator();
//        String connect = ites.next().toString();
//        String[] temps = connect.split(",");
//        float[] datas = new float[temps.length];
//        for (int i = 0; i < temps.length; i++) {
//            datas[i] = Float.parseFloat(temps[i]);
//        }
//
//        while (ites.hasNext()) {
//            connect = ites.next().toString();
//            temps = connect.split(",");
//            for (int i = 0; i < temps.length; i++) {
//                datas[i] += Float.parseFloat(temps[i]);
//            }
//        }
//        String result = "";
//        for (int i = 0; i < datas.length; i++) {
//            result += String.valueOf(datas[i]) + ",";
//        }
//        result = result.substring(0, result.lastIndexOf(","));
//        context.write(key, new Text(result));

//        System.out.println("Combiner key:" + key.toString());
//        Iterator<Text> ites = values.iterator();
//        try {
//            String[] messages = null;
//            int count = 0;
//            while (ites.hasNext()) {
//                String value = ites.next().toString();
//                String[] temp = value.split("#");
//                if (count == 0) {
//                    messages = temp;
//                } else {
////                    if (messages[0].trim() != temp[0].trim()) {
////                        messages[0] += "$" + temp[0];
////                        messages[1] += "$" + temp[1];
////                    }else{
////
////                    }
//                    messages[0] += "$" + temp[0];
//                    messages[1] += "$" + temp[1];
//                }
//                count++;
//            }
//            String result = "";
//            for (int i = 0; i < messages.length; i++) {
//                result += messages[i] + "#";
//            }
//            result = result.substring(0, result.lastIndexOf("#"));
//            context.write(key, new Text(result));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        Iterator<Text> ites = values.iterator();
        String result = "";
        while (ites.hasNext()) {
            String temp = ites.next().toString();
            result += temp + "@";
        }
//        System.out.println("temp:" + result);
        result = result.substring(0, result.lastIndexOf("@"));
        context.write(key, new Text(result));
    }

    public int getInt(byte[] bytes, int offset) {
        return (0xff & bytes[0 + offset]) | (0xff00 & (bytes[1 + offset] << 8))
                | (0xff0000 & (bytes[2 + offset] << 16))
                | (0xff000000 & (bytes[3 + offset] << 24));
    }
}
