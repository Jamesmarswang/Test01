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
import org.apache.hadoop.util.GenericOptionsParser;

public class Test01 {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reducer(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws 
			InterruptedException, IOException, ClassNotFoundException {
		Configuration conf = new Configuration();

		// ������ļ���ַǰ�Զ���ӣ�hdfs://master:9000/
		// conf.set("fs.default.name", "hdfs://master:9000/");
		// conf.set("hadoop.job.user", "hadoop");
		// // ָ��jobtracker��ip�Ͷ˿ںţ�master��/etc/hosts�п�������
		// conf.set("mapred.job.tracker", "master:9001");
		// JobConf jobConf=new JobConf(conf);
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		// String[] otherArgs = new String[]{"input","input\\output"};

		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}

		// Job job = new Job(jobConf,"dds");
		Job job = Job.getInstance(conf, "Text01");
		job.setJarByClass(Test01.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// for (int i = 0; i < otherArgs.length - 1; ++i) {
		// FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		// }
		// FileOutputFormat.setOutputPath(job, new Path(
		// otherArgs[otherArgs.length - 1]));
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
