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
 * �������
        MapReduce�е�join��Ϊ�ü��֣������������ reduce side join��map side join��semi join �ȡ�reduce join ��shuffle�׶�Ҫ���д��������ݴ��䣬����ɴ���������IOЧ�ʵ��£���map side join �ڴ�����С��������ʱ�ǳ����� ��
        Map side join��������³������е��Ż������������ӱ��У���һ����ǳ��󣬶���һ����ǳ�С��������С�����ֱ�Ӵ�ŵ��ڴ��С��������ǿ��Խ�С���ƶ�ݣ���ÿ��map task�ڴ��д���һ�ݣ������ŵ�hash table�У���Ȼ��ֻɨ�������ڴ���е�ÿһ����¼key/value����hash table�в����Ƿ�����ͬ��key�ļ�¼������У������Ӻ�������ɡ�Ϊ��֧���ļ��ĸ��ƣ�Hadoop�ṩ��һ����DistributedCache��ʹ�ø���ķ������£�
                  ��1���û�ʹ�þ�̬����DistributedCache.addCacheFile()ָ��Ҫ���Ƶ��ļ������Ĳ������ļ���URI�������HDFS�ϵ��ļ�������������hdfs://jobtracker:50030/home/XXX/file����JobTracker����ҵ����֮ǰ���ȡ���URI�б�������Ӧ���ļ�����������TaskTracker�ı��ش����ϡ�
                  ��2���û�ʹ��DistributedCache.getLocalCacheFiles()������ȡ�ļ�Ŀ¼����ʹ�ñ�׼���ļ���дAPI��ȡ��Ӧ���ļ���
                    ����������У������������С�ı�(����dept ���������ڴ��У���Mapper�׶ζ�Ա�����ű��ӳ��ɲ������ƣ���������Ϊkey�����Reduce�У���Reduce�м��㰴�ղ��ż���������ŵ��ܹ��ʡ�
                 
 */
public class Q1SumDeptSalary extends Configured implements Tool{

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{

		//���ھ���dept�ļ��еĔ���
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
			// ��ͬһ���T�ĆT�����Y�M�����
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
