


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramBuilder {

    public static class NGramMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        public int NumOfGram;
        @Override
        protected void setup(Context context) {
            Configuration conf=context.getConfiguration();
            //if the input format is false,then we set it as default value
            NumOfGram=conf.getInt("N",5);
        }
        @Override
        protected  void map(LongWritable key,Text value,Context context) throws  InterruptedException,IOException{
             //input value here is a sentence
            String content=value.toString();
            content=content.trim().toLowerCase();
            content=content.replaceAll("[^a-z]"," ");
            String[] group=content.split("\\s+");
            //we actually start from building 2 gram to N gram
            if(group.length<2){
                return;
            }
            int actualGram=0;
            if(group.length>NumOfGram){
                actualGram=NumOfGram;
            }
            else{
                actualGram=group.length;
            }
            int count=2;
            while(count<=actualGram){
                for(int i=0;i<=group.length-count;i++){
                    StringBuilder sb=new StringBuilder();
                    for(int time=0;time<count;time++){
                    	 if(group[i+time].length()>0){
                    	 	sb.append(group[i+time]);
                            sb.append(" ");
                    	 }
                         
                    }
                    String temp=sb.toString().trim();
                    context.write(new Text(temp),new IntWritable(1));
                }
                count++;
            }
        }
    }

    public static class NGramReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
         @Override
        protected  void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
              int sum=0;
              for(IntWritable value:values){
                   sum+=value.get();
              }
              context.write(key,new IntWritable(sum));
         }
    }
}
