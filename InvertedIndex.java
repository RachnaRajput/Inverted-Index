import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InvertedIndex {

  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text>{


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
    	filename = filename.replace(".txt", "");
        String line = value.toString();
        String words[] = line.split(" ");
        for(String s:words){
        	context.write(new Text(s),  new Text(filename));

        }
    	
    }
  }

  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

    	HashMap<String, Integer> hm = new HashMap<String,Integer>();
    	int count = 0;
    	for(Text t:values){
    		String str = t.toString();
    		if(hm!=null && hm.get(str)!=null){
    			count = (int)hm.get(str);
    			hm.put(str,++count);
    		}
    		else{
    			hm.put(str,1);
    		}
    	}
    	StringBuffer br= new StringBuffer();
    	for(String mtr: hm.keySet()){
    	br.append(mtr + ":" +hm.get(mtr)+"\t");
    	
    	}
    	context.write(key, new Text(br.toString()));

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "word count");
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setJarByClass(InvertedIndex.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}