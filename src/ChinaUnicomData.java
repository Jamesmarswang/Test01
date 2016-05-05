import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChinaUnicomData {// 统计每个个体在每一个点的通话次数、通话的时间数、每个点对应的天数、总的通话次数（通话总次数）

	public static class ChinaMap extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			String id = tokens[0].trim();
			String time = tokens[1];
			// SimpleDateFormat sdf = new
			// SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
			// Date d = sdf.parse(timeTemp);
			// String time = String.format(format, args)
			String xypoint = tokens[3] + "-" + tokens[2];
			if (!id.equals(id)) {
				context.write(new Text(id), new Text(time + "," + xypoint));
			}
		}
	}

	public static class ChinaReduce1 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<String> list = new ArrayList<String>();
			Iterator<Text> it = values.iterator();
			while (it.hasNext()) {
				String temp = it.next().toString();
				list.add(temp);
			}

			Collections.sort(list, new Comparator<String>() {

				@Override
				public int compare(String s1, String s2) {
					// TODO Auto-generated method stub
					String temp1 = s1.split(",")[0].toString();
					String temp2 = s2.split(",")[0].toString();
					return temp1.compareTo(temp2);
				}
			});

			String Str = "";
			for (int i = 0; i < list.size(); i++) {
				String s = list.get(i).toString().trim();
				Str += s;
				Str += "\t";
			}

			context.write(key, new Text(Str + list.size()));

		}
	}

	public static class ChinaReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> value = values.iterator();

			Map<String, List<String>> map = new HashMap<String, List<String>>();
			int AllRecrd = 0;// 记录个体总的记录数目
			while (value.hasNext()) {
				AllRecrd++;
				List<String> list = new ArrayList<String>();
				String temp = value.next().toString();
				String xypoint = temp.substring(temp.indexOf("||") + 2);
				String time = temp.substring(0, temp.indexOf("||"));
				if (map.containsKey(xypoint)) {
					List<String> l = map.get(xypoint);
					l.add(time);
					map.put(xypoint, l);
				} else {
					list.add(time);
					map.put(xypoint, list);
				}

			}

			String str = "";

			for (Entry<String, List<String>> em : map.entrySet()) {
				List<String> l = em.getValue();
				Collections.sort(l);
				int DayOfPerPoint = 1;// 记录每个用户对应的每个点的天数
				int HourOfPerPoint = 1;// 记录每个用户对应每个点的Hour数
				int CishuOfPerPoint = 1;// 记录每个用户对应每个点的次数（通话次数）
				for (int i = 0; i < l.size() - 1; i++) {
					String[] tokens1 = l.get(i).trim().split(" |:");
					String[] tokens2 = l.get(i + 1).trim().split(" |");
					if (!l.get(i + 1).equals(i)) {
						CishuOfPerPoint++;
					}
					if (!tokens2[0].equals(tokens1[0])) {
						DayOfPerPoint++;
					}
					if (!(tokens2[0] + "-" + tokens2[1]).equals(tokens1[0]
							+ "-" + tokens1[1])) {
						HourOfPerPoint++;
					}
				}
				String temp = em.getKey() + ";" + DayOfPerPoint + "-"
						+ HourOfPerPoint + "-" + CishuOfPerPoint;
				str += temp;
				str += "\t";
			}

			context.write(key, new Text(str + String.valueOf(AllRecrd)));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ChinaUnicomData");
		job.setJarByClass(ChinaUnicomData.class);

		job.setMapperClass(ChinaMap.class);
		job.setReducerClass(ChinaReduce1.class);

		job.setNumReduceTasks(10);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
