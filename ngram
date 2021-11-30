import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class Ngramstatistics {
	public static class Map1 extends Mapper<longwritable, text,="" intwritable=""> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		static int cnt = 0;
		List ls = new ArrayList();
		@SuppressWarnings("unchecked")
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer dt = new StringTokenizer(value.toString(), " ");
			
			while (dt.hasMoreTokens()) {
				ls.add(dt.nextToken());
			}
		
		}
		@Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
			int va = Integer.parseInt(context.getConfiguration().get("grams"));
			StringBuffer str = new StringBuffer("");
			for (int i = 0; i < ls.size() - va; i++) {
				int k=i;
				for(int j=0;j<va;j++) {="" if(j="">0) {
						str = str.append(" ");
						str = str.append(ls.get(k));
						
					} else 
					{
						str = str.append(ls.get(k));
						
					}
					k++;
				}
				word.set(str.toString());
				str=new StringBuffer("");
				//one.set(ls.size());
				context.write(word, one);
			}
	    }
	}
 
	public static class Reduce1 extends Reducer<text, intwritable,="" text,="" intwritable=""> {
		public void reduce(Text key, Iterable values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
 
	public static void main(String[] args) throws Exception {
		FileUtil.fullyDelete(new File(args[1]));
 
		Configuration conf = new Configuration();
		conf.set("grams", args[2]);
		Job job = new Job(conf, "wordcount");
		// job.setNumReduceTasks(0);
		job.setJarByClass(TreeDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);
 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map1.class);
		job.setReducerClass(Reduce1.class);
		job.waitForCompletion(true);
		System.out.println("Done.");
 
	}
}
