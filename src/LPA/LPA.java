package LPA;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class LPA {
    private static class LPAMapper extends Mapper<Object, Text, Text, Text> {
        private String getMaxLabel(HashMap<String,Double> LabelWeight){
            double Max=0;
            String out=null;
            for(String s:LabelWeight.keySet()){
                if(LabelWeight.get(s)>Max){
                    out=s;
                    Max=LabelWeight.get(s);
                }
            }
        return out;
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            HashMap<String, Double> labelAndWeight = new HashMap<>();
            ArrayList<String> Names = new ArrayList<>();
            String[] line = value.toString().split("\t");
            String list=line[1].split("&")[1];
            String[] nameAndWeightSet = list.split(";");
            for (String s : nameAndWeightSet) {
                if (s.length() > 0) {
                    String Label;
                    String[] nameAndWeight = s.split(",");
                    String Name = nameAndWeight[0];
                    Label = nameAndWeight[2];
                    double Weight = Double.valueOf(nameAndWeight[1]);
                    Double weightSum = labelAndWeight.get(Label);
                    if (weightSum == null)
                        weightSum = Weight;
                    else
                        weightSum = weightSum + Weight;
                    if(!Names.contains(Name))
                        Names.add(Name);
                    labelAndWeight.put(Label, weightSum);
                }
            }

            String LabelOfMax=getMaxLabel(labelAndWeight);
            if (!(LabelOfMax == null)) {
                context.write(new Text(line[0]), new Text(LabelOfMax + "&" + list));
                for (String n : Names) {
                    context.write(new Text(n), new Text(line[0] + "," + LabelOfMax));
                }
            }
        }
    }


    private static class LPAReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String list = null;
            String Label = null;
            HashMap<String, String> nameAndLabel = new HashMap<>();
            for (Text text : values) {
                String s = text.toString();
                if (s.contains("&")) {
                    list = s.split("&")[1];
                    Label = s.split("&")[0];
                } else {
                    nameAndLabel.put(s.split(",")[0], s.split(",")[1]);
                }
            }
            if (list != null && Label != null) {
                StringBuilder out = new StringBuilder();
                out.append(Label);
                out.append("&");
                String[] tempList = list.split(";");
                for (String s : tempList) {
                    String[] temp = s.split(",");
                    out.append(temp[0]).append(","); // name
                    out.append(temp[1]).append(","); // weight
                    out.append(nameAndLabel.get(temp[0])); // label
                    out.append(";");
                }
                context.write(key, new Text(out.toString()));
            }
        }
    }

    public static void run(int loopTimes, String input, String output) throws Exception {
        if (loopTimes <= 0) {
            System.out.println("loopTimes ERROR,loopTimse should be bigger than 1");
            return;
        }
        String in = input;
        String out, info;
        for (int i = 1; i <= loopTimes; ++i) {
            info = "LPA LOOP" + i;
            out = output + "/LOOP" + i;
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, info);
            job.setJarByClass(LPA.class);
            job.setMapperClass(LPAMapper.class);
            job.setReducerClass(LPAReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(in));
            FileOutputFormat.setOutputPath(job, new Path(out));
            job.waitForCompletion(false);
            in = out;
        }
        LPACleanup.run(output + "/LOOP" + loopTimes, output + "/result");
    }

    public static void main(String args[]) throws Exception {
        if(args.length==3)
            LPA.run(Integer.valueOf(args[2]), args[0], args[1]);
        else{
            LPA.run(5,"NormLPA","LPAresult");
        }
    }
}


