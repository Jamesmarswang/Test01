package TestPro;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 问题分析
        MapReduce中的join分为好几种，比如有最常见的 reduce side join、map side join和semi join 等。reduce join 在shuffle阶段要进行大量的数据传输，会造成大量的网络IO效率低下，而map side join 在处理多个小表关联大表时非常有用 。
        Map side join是针对以下场景进行的优化：两个待连接表中，有一个表非常大，而另一个表非常小，以至于小表可以直接存放到内存中。这样我们可以将小表复制多份，让每个map task内存中存在一份（比如存放到hash table中），然后只扫描大表：对于大表中的每一条记录key/value，在hash table中查找是否有相同的key的记录，如果有，则连接后输出即可。为了支持文件的复制，Hadoop提供了一个类DistributedCache，使用该类的方法如下：
                  （1）用户使用静态方法DistributedCache.addCacheFile()指定要复制的文件，它的参数是文件的URI（如果是HDFS上的文件，可以这样：hdfs://jobtracker:50030/home/XXX/file）。JobTracker在作业启动之前会获取这个URI列表，并将相应的文件拷贝到各个TaskTracker的本地磁盘上。
                  （2）用户使用DistributedCache.getLocalCacheFiles()方法获取文件目录，并使用标准的文件读写API读取相应的文件。
                    在下面代码中，将会把数据量小的表(部门dept ）缓存在内存中，在Mapper阶段对员工部门编号映射成部门名称，该名称作为key输出到Reduce中，在Reduce中计算按照部门计算各个部门的总工资。
                 
 */
public class Q1SumDeptSalary extends Configured implements Tool{

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{

		//用于緩存dept文件中的數據
		private Map<String,String> deptMap = new HashMap<String,String>();
		private String[] kv;
		
		public void setup(Context context){
			BufferedReader in = null;
			try {
				Path[] paths = Job.getInstance(context.getConfiguration()).getLocalCacheFiles();
				String deptIDName = null;
				for (Path path: paths) {
					if (path.toString().contains("dept")) {
						in = new BufferedReader(new FileReader(path.toString()));
						while (null != (deptIDName = in.readLine())) {
							deptMap.put(deptIDName.split(",")[0],deptIDName.split(",")[1]);
						}
					}
				}
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}finally
			{
				try {
					if (in != null) {
						in.close();
					}
				} catch (Exception e2) {
					// TODO: handle exception
					e2.printStackTrace();
				}
			}
			
		}
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			kv = value.toString().split(",");
			if (deptMap.containsKey(kv[7])) {
				if (null != kv[5] && "".equals(kv[5].toString())) {
					context.write(new Text(deptMap.get(kv[7].trim())), new Text(kv[5].trim()));
				}
			}
		}

	}
	
    public static class MapReduce extends Reducer<Text, Text, Text, LongWritable>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			// 對同一部門的員工工資進行求和
			long sumSalary = 0;
			for (Text val : values) {
				sumSalary += Long.parseLong(val.toString());
			}
			context.write(key, new LongWritable(sumSalary));
		}
    
    }	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Q1SumDeptSalary(),args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Q1SumDeptSalary");
		job.setJarByClass(Q1SumDeptSalary.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(MapReduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		job.addCacheFile(new Path(otherArgs[0]).toUri());
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		job.waitForCompletion(true);
		return job.isSuccessful() ? 0:1;
	}

}
