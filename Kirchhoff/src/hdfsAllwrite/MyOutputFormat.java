package hdfsAllwrite;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutputFormat extends FileOutputFormat<Text, Text> {

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        Path output = FileOutputFormat.getOutputPath(context);
        String file = context.getTaskAttemptID().getTaskID().toString();
        String path = output + "/"
                + file.substring(file.lastIndexOf("_") + 1, file.length());
        return new CustomRecordWriter(path);
    }

}
