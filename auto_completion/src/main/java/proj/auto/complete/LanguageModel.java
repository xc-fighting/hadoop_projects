package proj.auto.complete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

public class LanguageModel {
     public static class LanguageMap extends Mapper<Text,LongWritable,Text,Text>{
          int threshold;
          @Override
          public void setup(Context context){
               Configuration conf=context.getConfiguration();
               threshold=conf.getInt("threshold",20);
          }


          protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
                  String input=value.toString();
                  //split the input with \t
                  String[] group=input.split("\t");
                  if(group.length<2){
                       return;
                  }
                  //get the frequency
                  int freq=Integer.parseInt(group[1]);
                  if(freq<threshold){
                       return;
                  }
                  //then split the word
                  String[] items=group[0].split("\\s+");
                  StringBuilder sb=new StringBuilder();
                  for(int i=0;i<items.length-1;i++){
                       sb.append(items[i]+" ");
                  }
                  String new_key=sb.toString();
                  new_key=new_key.trim();
                  String new_value=items[items.length-1]+"="+freq;
                  context.write(new Text(new_key),new Text(new_value));
          }
		   
     }

     public static class LanguageReducer extends Reducer<Text,Text,DBOutputWritable,NullWritable>{
           //collect all of the key--value pairs with the same key
          //for example this is   ===> boy=20   girl=40
          //then we put all of these input the db
          //remember our mysql db only store the frequency which larger than threshold
          //only store top k into the db
          public class info{
               public String word;
               public int freq;
               public info(String w,int f){
                    this.word=w;
                    this.freq=f;
               }
           }
          int n;
          @Override
          public void setup(Context context){
               Configuration conf=context.getConfiguration();
               n=conf.getInt("k",5);
          }
          @Override
          public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
                //if appears since problem, modify the module in the project setting
               PriorityQueue<info> pq=new PriorityQueue<info>(new Comparator<info>() {
                    public int compare(info o1, info o2) {
                         return o1.freq-o2.freq;
                    }
               });
               //then below is top k method for solving this
               for(Text temp:values){
                   String str=temp.toString();
                   str=str.trim();
                   String[] arr=str.split("=");
                   String heading=arr[0];
                   int freq=Integer.parseInt(arr[1]);
                   if(pq.size()<n){
                       pq.offer(new info(heading,freq));
                   }
                   else{
                       info top=pq.peek();
                       if(freq>top.freq){
                           pq.poll();
                           pq.offer(new info(heading,freq));
                       }
                   }
               }
               while(pq.isEmpty()==false){
                   info top=pq.poll();
                   context.write(new DBOutputWritable(key.toString(),top.word,top.freq),NullWritable.get());
               }
          }

     }
}
