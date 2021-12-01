import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import java.util.*;
import java.io.*;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class triGram {

  public static class NGMapper extends Mapper<Object, Text, Text, IntWritable>
  {

    private final static IntWritable one = new IntWritable(1);
    private Text ngram = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
        String[] words = value.toString().toLowerCase().replaceAll("[^a-zA-Z ]", "").trim().split("\\s+");
        //Removing all punctuation and ignoring capitalization and splitting by spaces    
        for (int i=0; i<words.length-2; i++){
            ngram.set(words[i] + " " + words[i+1] + " " + words[i+2]);
            context.write(ngram, one);
	}
    }
  }

 /* public class Converger extends CombineFileInputFormat
  {
	public Converger()
	{
	// setting block size to 128mb which is the Cloudera default HDFS size
	this.setMaxSplitSize(134217728L);
	}
  }	*/

  public static class NGReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
	

	
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    // 			creating configuration for first job and compressing the map output
    Job job = Job.getInstance(conf, "tri-gram");
    conf.set("mapreduce.input.fileinputformat.split.maxsize","134211778L");
    job.setInputFormatClass(CombineTextInputFormat.class);	  
	  
    job.setJarByClass(triGram.class);
    job.setMapperClass(NGMapper.class);
    job.setCombinerClass(NGReducer.class);
    job.setReducerClass(NGReducer.class);
	  
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
//    job.setNumReduceTasks(1);
 //  job.setInputFormatClass(Converger.class);
	  
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
	  
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
