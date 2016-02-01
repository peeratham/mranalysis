package cs.vt.analysis.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cs.vt.analysis.analyzer.BlockAnalyzer;
import cs.vt.analysis.analyzer.visitor.BlockCounter;
import cs.vt.analysis.analyzer.visitor.DownUp;
import cs.vt.analysis.analyzer.visitor.Identity;
import cs.vt.analysis.analyzer.visitor.Stop;
import cs.vt.analysis.analyzer.visitor.Visitor;



public class SmallFilesToSequenceFileConverter extends Configured
implements Tool {
	
	static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, IntWritable> {
		private Text filenameKey;
		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			filenameKey = new Text(path.toString());
		}
		@Override
		protected void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			BlockAnalyzer blockAnalyzer = new BlockAnalyzer();
			Visitor v = new BlockCounter();
			blockAnalyzer.setVisitor(new DownUp(v, new Stop(), new Identity()));
			blockAnalyzer.setStringInput(new String(value.copyBytes()));
			blockAnalyzer.analyze();
			IntWritable result = new IntWritable(((BlockCounter)v).getCount());
			Text id = new Text(blockAnalyzer.getProject().getProjectID()+"");
			context.write(id, result);
		}	
	}
	
	static class Reduce extends Reducer<Text, BytesWritable, Text, IntWritable> {

	    @Override
	    public void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
	    	
		    BytesWritable jsonBytes = values.iterator().next();
		    String decoded = new String(jsonBytes.copyBytes(), "UTF-8");
	     	BlockAnalyzer blockAnalyzer = new BlockAnalyzer();
			Visitor v = new BlockCounter();
			blockAnalyzer.setVisitor(new DownUp(v, new Stop(), new Identity()));
			blockAnalyzer.setStringInput(decoded);
			blockAnalyzer.analyze();
			IntWritable result = new IntWritable(((BlockCounter)v).getCount());

			key.set(blockAnalyzer.getProject().getProjectID()+"");
			context.write(key, result);
	    }
	}
	
	@Override
	public int run(String[] args) throws Exception {

		 Path inputPath = new Path(args[0]);
		 Path outputPath = new Path(args[1]);

		 Configuration conf = getConf();
		 Job job = new Job(conf, this.getClass().toString());

		 FileInputFormat.setInputPaths(job, inputPath);
		 FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setJobName("SmallFilesToSequenceFileConverter");
		job.setJarByClass(SmallFilesToSequenceFileConverter.class);
		
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(SequenceFileMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	} 
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf,new SmallFilesToSequenceFileConverter(), args);
		System.exit(exitCode);
	}

}

