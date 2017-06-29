
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib. input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class FourTwoTwo
{
public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
{
	
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
		String[] splits = value.toString().split("\\|");
		String Company=splits[0]; 
		if(Company.equals("Onida")){
			Text word =new Text(splits[3].toString());
			

			context.write(word, new IntWritable(1));
		}
		
			
	
		
	}
}

public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
{
	@Override
	

	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		int sum = 0;
		while(values.iterator().hasNext())
		{
			sum+=values.iterator().next().get();
		}
		context.write(key, new IntWritable(sum));
	}
}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 

	{

Configuration conf = new Configuration();
Job job = new Job(conf,"MapReduce-1");
job.setJarByClass(FourTwoTwo.class);

job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);

job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);

job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

job.waitForCompletion(true);
	}

}

