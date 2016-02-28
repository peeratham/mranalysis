package cs.vt.analysis.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;

import cs.vt.analysis.analyzer.BlockAnalyzer;

public class SmallFilesToSequenceFileConverter extends Configured implements
		Tool {



	static class SequenceFileMapper extends
			Mapper<NullWritable, BytesWritable, Text, Text> {
		private Text filenameKey;
		private MultipleOutputs<Text, Text> multipleOutputs;
		

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			filenameKey = new Text(path.toString());
			multipleOutputs = new MultipleOutputs(context);
		}

		@Override
		protected void map(NullWritable key, BytesWritable value,
				Context context) throws IOException, InterruptedException {
			BlockAnalyzer blockAnalyzer = new BlockAnalyzer();
			JSONObject report = blockAnalyzer.analyze(new String(value.copyBytes()));
			Text result = new Text(report.toJSONString());
			Text id = new Text(blockAnalyzer.getProjectID() + "");
			multipleOutputs.write(id, result, id.toString());
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			multipleOutputs.close();
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
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class); //prevent part-m-xxx being created
		
		job.setMapperClass(SequenceFileMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf,
				new SmallFilesToSequenceFileConverter(), args);
		System.exit(exitCode);
	}

}
