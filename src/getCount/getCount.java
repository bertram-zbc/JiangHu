package getCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 统计共现次数
 */
public class getCount {

    private static class WeightMapper extends Mapper<Object,Text,Text,Text>{
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
            String s = value.toString();
            //s = s.substring(0,s.length()-1);
            String []temp = s.split("\t");
            for(int i=0;i<temp.length;i++){
                for (int j=0;j<temp.length;j++){
                    if(i!=j){
                        Text mapKey = new Text();
                        String str = temp[i]+","+temp[j]+"\t";
                        mapKey.set(str);
                        Text mapValue = new Text();
                        mapValue.set("1");
                        context.write(mapKey,mapValue);
                    }
                }
            }
        }
    }

    public static class WeightReducer extends Reducer<Text,Text,Text,IntWritable>{
        public void reduce(Text key,Iterable<Text>values,Context context) throws IOException, InterruptedException {
            int sum=0;
            for (Text val:values){
                sum++;
            }
            IntWritable result = new IntWritable(sum);
            context.write(key,result);
        }
    }

    public static void run(String input,String output) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = new Job(conf, "Count");
        job1.setJarByClass(getCount.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapperClass(getCount.WeightMapper.class);
        job1.setReducerClass(getCount.WeightReducer.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output));
        job1.waitForCompletion(true);
    }

    public static void main(String []args) throws Exception{
        if(args.length!=2) {
            System.out.println("input path:name,output path:count");
            run("name", "count");
        }else{
            run(args[0],args[1]);
        }
    }

}
