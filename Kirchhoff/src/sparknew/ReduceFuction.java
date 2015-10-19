package sparknew;

import org.apache.spark.api.java.function.Function2;


public class ReduceFuction implements Function2<String, String, String> {

    @Override
    public String call(String text1, String text2) throws Exception {
        // TODO Auto-generated method stub

        String[] str1 = text1.split(",");
        String[] str2 = text2.split(",");
        float[] values = new float[str1.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = Float.parseFloat(str1[i]) + Float.parseFloat(str2[i]);
        }

        String result = "";
        for (int i = 0; i < str1.length; i++) {
            result += values[i] + ",";
        }
        values = null;
        result = result.substring(0, result.lastIndexOf(","));
        return result;
    }

}
