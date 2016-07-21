package Driver;


import LPA.LPA;
import LPA.NormalizeForLPA;
import Normalize.Normalize;
import PageRank.NormalizeForPageRank;
import PageRank.PageRank;
import extractNames.extractNames;
import getCount.getCount;

import java.util.Scanner;


public class Driver {
    public static void main(String args[]) throws Exception {
        System.out.println("**usage:");
        System.out.println("**1.hadoop jar XXX.jar default(result path is output)");
        System.out.println("**2.hadoop jar XXX.jar [out path] num1 num2(num1 is the times of LPA,num2 is PageRank)");
        System.out.println("**3.hadoop jar XXX.jar single(run single job)");
        if(args.length==1&&args[0].equals("default")){
            System.out.println("**All result will be stored in output");
            System.out.println("**extract Name: input path:novel,output path:name");
            extractNames.run("novel","output/name");
            System.out.println("**get Count: input path:name,output path:count");
            getCount.run("output/name","output/count");
            System.out.println("**Normalize: input path:count,output path:Norm");
            Normalize.run("output/count","output/Norm");
            System.out.println("**Normalize For LPA: input pathha:count,output path:forLPA");
            NormalizeForLPA.run("output/count","output/forLPA");
            System.out.println("**LPA: input path:forLPA,output path LPAout,Times:5");
            LPA.run(5,"output/forLPA","output/LPAout");
            System.out.println("**Normalize For PageRank: input path:count,output path:forPR");
            NormalizeForPageRank.run("output/count","output/forPR");
            System.out.println("**PageRank: input path:forPR,output path:PRout,Times:5");
            PageRank.run(5,"output/forPR","output/PRout");
            System.out.println("**All missions done");
        }
        if(args.length==3&&!args[0].equals("default")){
            String outFile=args[0];
            int LPAtimes=Integer.valueOf(args[1]);
            int PRtimes=Integer.valueOf(args[2]);
            extractNames.run("novel",outFile+"/name");
            getCount.run(outFile+"/name",outFile+"/count");
            Normalize.run(outFile+"/count",outFile+"Norm");
            NormalizeForLPA.run(outFile+"/count",outFile+"/forLPA");
            NormalizeForPageRank.run(outFile+"/count",outFile+"/forPR");
            LPA.run(LPAtimes,outFile+"/forLPA",outFile+"/LPAout");
            PageRank.run(PRtimes,outFile+"/forPR",outFile+"/PRout");
            System.out.println("**All missions done");
        }
        if(args.length==1&&args[0].equals("single")){
            System.out.println("**usage:");
            System.out.println("**num,inpath,outpath,times(times is only useful for LPA and PageRank)");
            System.out.println("**num:1->extract names");
            System.out.println("**num:2->get Count");
            System.out.println("**num:3->Normalize");
            System.out.println("**num:4->LPA(LPA input path is the output of getCount)");
            System.out.println("**num:5->Page Rank(PageRank input path is the output of getCount)");
            Scanner in=new Scanner(System.in);
            String command=in.nextLine();
            String []commands=command.split(",");
            if(commands.length!=4&&commands.length!=3){
                System.out.println("**illegal input");
            }else{
                if(commands[0].equals("1")){
                    extractNames.run(commands[1],commands[2]);
                }
                else if(commands[0].equals("2")){
                    getCount.run(commands[1],commands[2]);
                }
                else if(commands[0].equals("3")){
                    Normalize.run(commands[1],commands[2]);
                }
                else if(commands[0].equals("4")){
                    NormalizeForLPA.run(commands[1],"forLPA");
                    LPA.run(Integer.valueOf(commands[3]),"forLPA",commands[2]);
                }
                else if(commands[0].equals("5")){
                    NormalizeForPageRank.run(commands[1],"forPR");
                    PageRank.run(Integer.valueOf(commands[3]),"forPR",commands[2]);
                }else{
                    System.out.println("**illegal input");
                }
            }
        }
    }
}
