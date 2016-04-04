package cs.vt.analysis.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

//credit: Felix Ren-Chyan Chern
//http://www.idryman.org/blog/2013/09/22/process-small-files-on-hadoop-using-combinefileinputformat-1/


public class CFInputFormat extends CombineFileInputFormat<FileLineWritable, Text> {
  public CFInputFormat(){
    super();
    setMaxSplitSize(67108864); // 64 MB, default block size on hadoop
  }
  public RecordReader<FileLineWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException{
    return new CombineFileRecordReader<FileLineWritable, Text>((CombineFileSplit)split, context, CFRecordReader.class);
  }
  @Override
  protected boolean isSplitable(JobContext context, Path file){
    return false;
  }
}
