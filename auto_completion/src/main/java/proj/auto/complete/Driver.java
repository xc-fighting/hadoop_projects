package proj.auto.complete;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver{

    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{
        Configuration conf=new Configuration();
        //set the dividor of input file with . as the seperator of each sentence
        conf.set("textinputformat.record.delimiter",".");
        //set the number of diagram
        conf.set("N",args[2]);
        //the first job used to generate the n-gram

        Job job1=Job.getInstance();
        job1.setJobName("n_gram");
        job1.setJarByClass(Driver.class);
        //set job1's mapper class and reducer class
        job1.setMapperClass(NGramBuilder.NGramMapper.class);
        job1.setReducerClass(NGramBuilder.NGramReducer.class);
        //set job1's output format class
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        //set job input,output format class
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        //set job input output path
        TextInputFormat.setInputPaths(job1,new Path(args[0]));
        TextOutputFormat.setOutputPath(job1,new Path(args[1]));
        //wait for job end
        job1.waitForCompletion(true);
        //end of job1
        //start of job2
        Configuration  conf2=new Configuration();
        conf2.set("threshold",args[3]);

        DBConfiguration.configureDB(conf2,"com.mysql,jdbc.Driver",
                "jdbc:mysql://127.0.0.1:port/test","root",
                "Xuchen123");

        Job job2=Job.getInstance();
        job2.setJobName("language_model");
        job2.setJarByClass(Driver.class);
        job2.addArchiveToClassPath(new Path("path_to_ur_connector"));

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(TextInputFormat.class);
        job2.setOutputValueClass(DBOutputFormat.class);

        job2.setMapperClass(LanguageModel.LanguageMap.class);
        job2.setReducerClass(LanguageModel.LanguageReducer.class);
    }
}
