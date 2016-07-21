package LPA;

public class labelWithWeight {
    String label;
    double weight;
    public labelWithWeight(){
        label="";
        weight=0;
    }
    public labelWithWeight(String l,double w){
        label=l;
        weight=w;
    }
    public double getWeight(){
        return weight;
    }
    public String getLabel(){
        return label;
    }
}
