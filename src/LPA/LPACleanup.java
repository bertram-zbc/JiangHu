package LPA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

class LPACleanup {
    private static class CleanMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            if (line[1].contains("&")) {
                String[] temp = line[1].split("&");
                context.write(new Text(temp[0]), new Text(line[0]));
            }
        }
    }

    private static class CleanReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                context.write(key, t);
            }
        }
    }

    public static void run(String inPath, String outPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LPA Final");
        job.setJarByClass(LPACleanup.class);
        job.setMapperClass(LPACleanup.CleanMapper.class);
        job.setReducerClass(LPACleanup.CleanReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.waitForCompletion(false);
    }
}

