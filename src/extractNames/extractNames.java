package extractNames;

import ReadFile.ReadPerson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 提取每一段中同现的名字
 */
public class extractNames {

    private static ArrayList<String> person = new ArrayList<>();

    private static class ExtractMapper extends Mapper<Object,Text,Text,Text>{

        public void setup(Context context) throws IOException {
            //person = ReadPerson.readToArray("person_list.txt");
            Configuration conf = context.getConfiguration();
            Path[]path = DistributedCache.getLocalCacheFiles(conf);
            BufferedReader reader = null;
            reader = new BufferedReader(new FileReader(path[0].toString()));
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (!line.equals("")) {
                    person.add(line);
                }
            }
            reader.close();
        }
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException {

//            FileSplit fileSplit = (FileSplit) context.getInputSplit();
//            String filename = fileSplit.getPath().getName();
            String line=value.toString();
            ArrayList<String> temp=new ArrayList<>();
            String outstring="";
            for (String aPerson : person) {
                Pattern pattern = Pattern.compile(aPerson);
                Matcher matcher = pattern.matcher(line);
                while (matcher.find()) {
                    if (!temp.contains(aPerson))
                        temp.add(aPerson);
                }
            }
            if(temp.size()>=2) {
                for (String Temp : temp) {
                    outstring = outstring + Temp + "\t";
                }
            }
            if(!outstring.equals("")){
                //outstring = outstring.substring(0,outstring.length()-1);
                context.write(new Text(outstring),new Text("1"));
            }
        }
    }

    private static class ExtractReducer extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key,Iterable<Text>values,Context context) throws IOException, InterruptedException {
            for(Text val:values){
                context.write(key,new Text(""));
            }
        }
    }

    public static void run(String input,String output)throws Exception{
        JobConf conf = new JobConf(extractNames.class);
        Job job1 = new Job(conf, "extract Names");
        DistributedCache.addCacheFile(new URI("person_list.txt"), job1.getConfiguration());
        job1.setJarByClass(extractNames.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapperClass(ExtractMapper.class);
        job1.setReducerClass(ExtractReducer.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output));
        job1.waitForCompletion(true);

    }

    public static void main(String[]args) throws Exception {
        if(args.length!=2) {
            System.out.println("input path:novel,output path:name");
            run("novel", "name");
        }else{
            run(args[0],args[1]);
        }
    }
}
