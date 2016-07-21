package PageRank;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PRCleanup {
    public static class CleanMapper extends Mapper<Object, Text, DoubleWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] terms = value.toString().split("\t"); // {name, PageRank#list}
            if (terms[1].contains("&")) { // {name, PageRank#list}
                String[] temp = terms[1].split("&"); // {PageRank, list}
                context.write(new DoubleWritable(Double.valueOf(temp[0])), new Text(terms[0]));
            }
        }
    }


    public static class CleanReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                context.write(key, t);
            }
        }
    }

    public static void run(String inPath, String outPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PR Cleanup");
        job.setJarByClass(PRCleanup.class);
        job.setMapperClass(PRCleanup.CleanMapper.class);
        job.setReducerClass(PRCleanup.CleanReducer.class);
        job.setSortComparatorClass(DescComparator.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.waitForCompletion(false);
    }
}
