import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Mypackage.UDFkey;

public class ChinaTelecom {

	public static class TelcCom extends Mapper<Object, Text, UDFkey, Text> {
		private final UDFkey k = new UDFkey();
		private final Text v = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] tokens = value.toString().split(",");
			String id = tokens[0].trim();
			String time = tokens[1].trim();
			k.setFirstKey(id);
			k.setSecondKey(time);
			v.set(time);

			context.write(k, v);
		}
	}

	public static class TeleResult extends Reducer<UDFkey, Text, Text, Text> {
		public void reduce(UDFkey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			SimpleDateFormat sdf = new SimpleDateFormat(
					"yyyy-MM-dd'T'HH:mm:ss.'000Z'");
			int num = 0;// 记录每个个体的记录数
			int count = 0;
			double timeminus = 0;
			Date date = null;
			List<Date> list = new ArrayList<Date>();
			Iterator<Text> it = values.iterator();
			while (it.hasNext()) {
				num++;
				try {
					date = sdf.parse(it.next().toString());
					list.add(date);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			Collections.sort(list);
			String str = " ";
			for (int i = 1; i < list.size(); i++) {
				timeminus = (double) (list.get(i).getTime() - list.get(i - 1)
						.getTime()) / (1000 * 60);
				if (timeminus > 0.0) {
					count++;
					String temp = String.valueOf(timeminus);
					str += (temp + "\t");
				}
			}
			context.write(new Text(key.getFirstKey()), new Text(str + "\t"
					+ num + "\t" + count));
		}
	}

	public static class MygroupCompare extends WritableComparator {
		public MygroupCompare() {
			super(UDFkey.class, true);
		}

		public int compare(WritableComparable W1, WritableComparable W2) {
			UDFkey s1 = (UDFkey) W1;
			UDFkey s2 = (UDFkey) W2;
			int temp = s1.getFirstKey().compareTo(s2.getFirstKey());
			if (temp == 0) {
				return s1.getSecondKey().compareTo(s2.getSecondKey());
			}
			return temp;
		}
	}

	public static class MyPartitioner extends Partitioner<UDFkey, Text> {

		@Override
		public int getPartition(UDFkey key, Text value, int numPartition) {
			// TODO Auto-generated method stub
			return Math.abs(key.getFirstKey().hashCode()) % numPartition;
		}

		// @Override
		// public int getPartition(Text key, Text value, int numPartition) {
		// String[] val = value.toString().split("'");
		// SimpleDateFormat sdf = new SimpleDateFormat(
		// "yyyy-MM-dd'T'HH:mm:ss.'000Z'");
		// Date d = null;
		// try {
		// d = sdf.parse(val[0]);
		// } catch (ParseException e) {
		// e.printStackTrace();
		// }
		// String day = String.format("%td", d);
		// if (Integer.parseInt(day) == 14) {
		// return 0;
		// } else if (Integer.parseInt(day) == 14) {
		// return 1;
		// } else {
		// return 3;
		// }
		// }
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ChinaTelecom");
		job.setJarByClass(ChinaTelecom.class);
		job.setMapperClass(TelcCom.class);
		// job.setCombinerClass(TeleResult.class);
		job.setReducerClass(TeleResult.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(MyPartitioner.class);
		job.setGroupingComparatorClass(MygroupCompare.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
