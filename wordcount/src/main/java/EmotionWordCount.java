import java.io.FileInputStream;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class EmotionWordCount {

    public static void main(String[] args) throws Exception{
        Configuration conf =new Configuration();
        String[] others=new GenericOptionsParser(conf,args).getRemainingArgs();
        if(others.length!=2){
             System.err.println("Usage:EmotionWordCount <in> <out>");
             System.exit(2);
        }
        //first:input path next:output path
        Job job=new Job(conf,"emotionword");
        job.setJarByClass(EmotionWordCount.class);
        job.setMapperClass(StringMapper.class);
        job.setCombinerClass(WordReducer.class);
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(others[0]));
        FileOutputFormat.setOutputPath(job,new Path(others[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
