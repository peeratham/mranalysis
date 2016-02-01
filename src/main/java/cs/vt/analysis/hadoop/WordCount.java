package cs.vt.analysis.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class WordCount extends Configured implements Tool {

	  public static void main(String args[]) throws Exception {
	    int res = ToolRunner.run(new WordCount(), args);
	    System.exit(res);
	  }

	  public int run(String[] args) throws Exception {
	    Path inputPath = new Path(args[0]);
	    Path outputPath = new Path(args[1]);

	    Configuration conf = getConf();
	    Job job = new Job(conf, this.getClass().toString());

	    FileInputFormat.setInputPaths(job, inputPath);
	    FileOutputFormat.setOutputPath(job, outputPath);

	    job.setJobName("WordCount");
	    job.setJarByClass(WordCount.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    job.setMapperClass(Map.class);
	    job.setCombinerClass(Reduce.class);
	    job.setReducerClass(Reduce.class);

	    return job.waitForCompletion(true) ? 0 : 1;
	  }

	  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();

	    @Override
	    public void map(LongWritable key, Text value,
	                    Mapper.Context context) throws IOException, InterruptedException {
	      String line = value.toString();
	      StringTokenizer tokenizer = new StringTokenizer(line);
	      while (tokenizer.hasMoreTokens()) {
	        word.set(tokenizer.nextToken());
	        context.write(word, one);
	      }
	    }
	  }

	  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	    @Override
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable value : values) {
	        sum += value.get();
	      }

	      context.write(key, new IntWritable(sum));
	    }
	  }

	}
