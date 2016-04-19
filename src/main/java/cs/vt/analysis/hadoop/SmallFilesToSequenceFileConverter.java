package cs.vt.analysis.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;

import cs.vt.analysis.analyzer.AnalysisManager;
import cs.vt.analysis.analyzer.analysis.AnalysisException;
import cs.vt.analysis.analyzer.parser.ParsingException;
import cs.vt.analysis.hadoop.WordCount.Reduce;

public class SmallFilesToSequenceFileConverter extends Configured implements
		Tool {

	static class SequenceFileMapper extends
			Mapper<FileLineWritable, Text, Text, Text> {
		private Text filenameKey;
//		private MultipleOutputs<NullWritable, Text> multipleOutputs;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
//			multipleOutputs = new MultipleOutputs(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
//			multipleOutputs.close();
		}

		@Override
		protected void map(FileLineWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			AnalysisManager blockAnalyzer = new AnalysisManager();
			JSONObject report = null;
			try {
				report = blockAnalyzer.analyze(value.toString());
				Text result = new Text(report.toJSONString());
				Text id = new Text(blockAnalyzer.getProjectID() + "");
				if(id!=null || report!=null){
					context.write(id, result);
				}
			} catch (ParsingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (AnalysisException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}

//			multipleOutputs.write(NullWritable.get(), result, id.toString());
		}
	}
	
	public static class MultiFileReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			String result = "";
			for (Text val: values) {
				result = val.toString();	//there is one val
			}
			context.write(key, new Text(result));
		}
		
		
	}

	@Override
	public int run(String[] args) throws Exception {

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Configuration conf = getConf();
		Job job = new Job(conf, this.getClass().toString());

		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setJobName("SmallFilesToSequenceFileConverter");
		job.setJarByClass(SmallFilesToSequenceFileConverter.class);

		// job.setInputFormatClass(WholeFileInputFormat.class);
		job.setInputFormatClass(CFInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class); // prevent
																			// part-m-xxx
																			// being
																			// created

		job.setMapperClass(SequenceFileMapper.class);
		job.setReducerClass(MultiFileReducer.class);
//		job.setNumReduceTasks(0);

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
