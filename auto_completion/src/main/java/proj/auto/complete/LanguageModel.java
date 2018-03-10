package proj.auto.complete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LanguageModel {
     public static class LanguageMap extends Mapper<Text,LongWritable,Text,Text>{
          int threshold;
          @Override
          public void setup(Context context){
               Configuration conf=context.getConfiguration();
               threshold=conf.getInt("threshold",20);
          }


          protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{

          }
		   
     }

     public static class LanguageReducer extends Reducer<Text,Text,DBOutputWritable,NullWritable>{

     }
}
