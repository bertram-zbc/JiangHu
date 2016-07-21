package PageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class PageRank {
    private static class PRMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");//name   PR&name,weight;name,weight...
            String[] temp = line[1].split("&");//PR&name,weight;name,weight...
            double PR = Double.valueOf(temp[0]);//PR
            context.write(new Text(line[0]), new Text(temp[1]));//name  name,weight;name,weight...
            String[] NameAndWeight = temp[1].split(";");
            for (String str : NameAndWeight) {
                if (str.length() > 0 ) {
                    double weight = Double.valueOf(str.split(",")[1]);
                    String person = str.split(",")[0];
                    context.write(new Text(person), new Text("$&" + (weight * PR)));//name  $
                }
            }
        }
    }

    private static class PRReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double PRsum = 0;
            String list = "";
//            for (Text v : values) {
//                String[] temp = v.toString().split("\t");
//                if (temp.length == 2) {
//                    PRsum += Double.valueOf(temp[1]);
//                } else {
//                    list = temp[0];
//                }
//            }
            for(Text v:values){
                if(v.toString().contains("$")){
                    String newPR=v.toString().split("&")[1];
                    PRsum=PRsum+Double.valueOf(newPR);
                }else{
                    list=v.toString();
                }
            }
            context.write(key, new Text(String.valueOf(1 - 0.8 + 0.8 * PRsum) + "&" + list));//0.8 Damping
        }
    }

    public static void run(int loopTimes, String inPath, String outPath) throws Exception {
        String in = inPath;
        String out, description;
        for (int i = 1; i <= loopTimes; ++i) {
            description = "PageRank LOOP" + i;
            out = outPath + "/LOOP" + i;

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, description);
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PageRank.PRMapper.class);
            job.setReducerClass(PageRank.PRReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(in));
            FileOutputFormat.setOutputPath(job, new Path(out));
            job.waitForCompletion(false);

            in = out;
        }
        PRCleanup.run(outPath + "/LOOP" + loopTimes, outPath + "/result");
    }


    public static void main(String[] args) throws Exception {
        PageRank.run( Integer.valueOf(args[2]), args[0], args[1]);
    }
}

