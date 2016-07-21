package LPA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;


public class NormalizeForLPA {

    public static class RelaMapper extends Mapper<Object,Text,Text,Text>{

        public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
            String []str = value.toString().split("\t\t");
            String []name = str[0].split(",");
            Text mapKey = new Text();
            mapKey.set(name[0]);
            String val = name[1]+","+str[1];
            Text mapValue = new Text();
            mapValue.set(val);
            context.write(mapKey,mapValue);
        }
    }

    public static class RelaReducer extends Reducer<Text,Text,Text,Text>{

        public void reduce(Text key,Iterable<Text>value,Context context) throws IOException, InterruptedException {
            int sum=0;
            ArrayList<Integer>count = new ArrayList<Integer>();
            ArrayList<String>name = new ArrayList<String>();
            for (Text val:value){
                String[] res = val.toString().split(",");
                sum+=Integer.parseInt(res[1]);
                count.add(Integer.parseInt(res[1]));
                name.add(res[0]);
            }
            String result = "";
            result=result+key.toString()+"&";
            for (int i=0;i<count.size();i++){
                float weight = (float)count.get(i)/sum;
                String temp = name.get(i)+","+weight+","+name.get(i)+";";
                result += temp;
            }
            Text mapValue = new Text();
            mapValue.set(result);
            context.write(key,mapValue);
        }
    }

    public static void run(String input,String output) throws Exception{
        Configuration conf = new Configuration();
        Job job1 = new Job(conf, "Normalize For LPA");
        job1.setJarByClass(NormalizeForLPA.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapperClass(RelaMapper.class);
        job1.setReducerClass(RelaReducer.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output));
        job1.waitForCompletion(true);
    }

    public static void main(String[]args) throws Exception{
        if(args.length!=2) {
            System.out.println("input path:count,output path:forLPA");
            run("count", "forLPA");
        }else{
            run(args[0],args[1]);
        }
    }
}