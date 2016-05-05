package MapReducePro;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.Collections;
import java.util.Iterator;

public class Test extends Configured implements Tool {
	// TODO Auto-generated method stub
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println(args.length);
			for (int i = 0; i < args.length; i++)
				System.err.println(args[i]);
			System.err
					.println("Usage:MaxTemperature <input path> <output path>");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new Test(), args);

		System.exit(res);
	}

	public static class DistinguishGridCarMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// LongWritable key�����кţ�Text value����ÿһ�е��ı�
			// System.out.println(value.toString());

			try {
				String line = value.toString();
				if (line.startsWith("λ��")) {

					String[] tokens = line.split(";");
					if (tokens.length > 5) {
						String[] carplatarr = tokens[0].split(":");
						String carplat = carplatarr[1];// ���ƺ�
						String[] lonarr = tokens[1].split(":");
						String[] latarr = tokens[2].split(":");
						String lon = lonarr[1];// ����
						String lat = latarr[1];// γ��
						String[] timearr = tokens[3].split(":");

						String[] timearrmiddle = timearr[1].split(" ");
						String time = timearrmiddle[1] + ":" + timearr[2] + ":"
								+ timearr[3];// ʱ��
						String date = timearrmiddle[0] + " " + timearrmiddle[1]
								+ ":" + timearr[2] + ":" + timearr[3];
						String[] speedarr = tokens[4].split(":");
						String speed = speedarr[1];// �ٶ�
						// System.out.println("!!!!!!!!!!!!!!"+time);
						double lonf = 113.758683;
						double lone = 114.621249;
						double latf = 22.862324;
						double late = 22.442601;
						double eachlon = 0.015683018;
						double eachlat = 0.014473207;
						// double lonnum =
						// Math.abs((Double.valueOf(lon)/1000000-lonf))/eachlon;
						// if(){
						//
						// }
						// gridNumber ������
						// int gridNumber = (int)
						// (Math.abs((Double.valueOf(lon)/1000000-lonf))/eachlon+1+(Math.abs((Double.valueOf(lat)/1000000-latf))/eachlat)*60);
						// System.out.println("!!!!!!!!!!!!!!"+carplat+"_"+date);
						int gridNumber = (int) ((Math
								.abs((Double.valueOf(lon) / 1000000 - lonf))
								/ eachlon + 1) + ((int) (Math.abs((Double
								.valueOf(lat) / 1000000 - latf)) / eachlat)) * 55);
						context.write(
								new Text(carplat + "_" + date + "_"
										+ timearrmiddle[1]),
								new Text(Double.valueOf(lon) / 1000000 + ","
										+ (Double.valueOf(lat)) / 1000000 + ","
										+ time + "," + speed + "," + gridNumber));

					}
				}

			} catch (Exception e) {
				// TODO: handle exception
				System.out.println(value.toString());
			}

			// context.write(new Text(carplat),new Text(lat+","+lon));
		}
	}

	public static class DistinguishGridCarReducer extends
			Reducer<Text, Text, Text, Text> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> it = values.iterator();
			// String carplat ;//���ƺ�
			String lonf = null;// ��һ���㾭��
			String latf = null;// ��һ����γ��
			String[] timef = null;// ��һ����ʱ��
			String speed = null;// ��һ�����ٶ�
			int gridNumberf = 0;// ��һ�������ڸ�����
			String lone = null;// �յ㾫��
			String late = null;// �յ�γ��
			String[] timee = null;// �յ�ʱ��
			int gridNumbere = 0;// �յ����ڸ�����
			int timeend = 0;// ����ʱ�䣨�룩
			int timefirst = 0;// ��ֹʱ�䣨�룩
			String linef = it.next().toString();
			String[] tokensf = linef.split(",");
			lonf = tokensf[0];
			latf = tokensf[1];
			timef = tokensf[2].split(":");
			timefirst = Integer.valueOf(timef[0]) * 3600
					+ Integer.valueOf(timef[1]) * 60
					+ Integer.valueOf(timef[2]);
			// System.out.println("fffffffff"+timefirst);
			gridNumberf = Integer.parseInt(tokensf[4]);
			int time = 0;
			int hr = 0;
			while (it.hasNext()) {
				String line = it.next().toString();
				String[] tokens = line.split(",");
				lone = tokens[0];
				late = tokens[1];
				timee = tokens[2].split(":");
				timeend = Integer.valueOf(timee[0]) * 3600
						+ Integer.valueOf(timee[1]) * 60
						+ Integer.valueOf(timee[2]);
				gridNumbere = Integer.parseInt(tokens[4]);
				if (gridNumbere == gridNumberf) {
					time = timeend - timefirst;
					// if(time==0){
					// System.out.println(timeend+"   "+timefirst);
					// }
					if (gridNumbere < 1620) {
						context.write(new Text(key),
								new Text(String.valueOf(gridNumbere) + ","
										+ String.valueOf(time)));
						// System.out.println(key+","+String.valueOf(gridNumbere)+","+String.valueOf(time));
					}

				} else {
					if (lone == null) {
						lone = lonf;
					}
					if (late == null) {
						late = latf;
					}
					// ����
					int latmiddlef = gridNumberf % 60;
					int latmiddlee = gridNumbere % 60;
					int lonmiddlef = gridNumberf / 60;
					int lonmiddlee = gridNumbere / 60;
					int xsize = Math.abs(latmiddlef - latmiddlee);
					int ysize = Math.abs(lonmiddlee - lonmiddlef);
					double[] x = new double[xsize];// ����
					double[] y = new double[ysize];// ����
					double gridlonf = 113.758683;
					double gridlatf = 22.442601;
					int n = 0;
					for (int i = latmiddlef; i < latmiddlee; i++) {

						x[n] = gridlonf + i * 0.015683018;

						n++;
					}
					int m = 0;
					for (int i = lonmiddlef; i < lonmiddlee; i++) {
						y[m] = gridlatf + i * 0.014473207;
						m++;
					}
					double xpoint = 0.0;
					double ypoint = 0.0;
					List<point> pointList = new ArrayList<point>();// ��������

					// �󽻵�����ķ�����
					for (int i = 0; i < m; i++) {
						// System.out.println("the"+i+"number"+y[i]+"XXXXXXX"+y.length+"m.lenth"+m);
						ypoint = Math.abs(y[i] - Double.valueOf(latf))
								* Math.abs(Double.valueOf(late)
										- Double.valueOf(latf))
								/ Math.abs(Double.valueOf(lone)
										- Double.valueOf(lonf))
								+ Double.valueOf(lonf);
						point p = new point();
						p.setX(y[i]);
						p.setY(ypoint);
						pointList.add(p);

					}

					for (int i = 0; i < n; i++) {
						// System.out.println("the"+i+"number"+x[i]+"XXXXXXX"+x.length+"n.lenth"+n);
						xpoint = Math.abs(x[i] - Double.valueOf(lonf))
								* Math.abs(Double.valueOf(lone)
										- Double.valueOf(lonf))
								/ Math.abs(Double.valueOf(late)
										- Double.valueOf(latf))
								+ Double.valueOf(latf);
						point p = new point();
						p.setX(xpoint);
						p.setY(x[i]);
						pointList.add(p);
					}

					// ȥ�����ڷ�Χ�ڵĵ�
					for (int i = 0; i < pointList.size(); i++) {
						if (pointList.get(i).getX() < Double.valueOf(late)
								|| pointList.get(i).getX() > Double
										.valueOf(latf)) {
							pointList.remove(pointList.get(i));
						}

					}
					// �����㰴��С�����˳������
					Collections.sort(pointList);
					//
					for (int i = 0; i < pointList.size(); i++) {
						if (i != (pointList.size() - 1)) {
							double firstX = Double.valueOf(pointList.get(i)
									.getX());
							double firstY = Double.valueOf(pointList.get(i)
									.getY());
							double endX = Double.valueOf(pointList.get(i + 1)
									.getX());
							double endY = Double.valueOf(pointList.get(i + 1)
									.getY());
							// System.out.println("fX"+firstX);
							// System.out.println("fY"+firstY);
							// System.out.println("EX"+endX);
							// System.out.println("EY"+endY);
							double distance = Math.sqrt((firstX - endX)
									* (firstX - endX) + (firstY - endY)
									* (firstY - endY));
							double distancewhole = Math.sqrt((pointList.get(0)
									.getX() - pointList.get(
									pointList.size() - 1).getX())
									* (pointList.get(0).getX() - pointList.get(
											pointList.size() - 1).getX())
									+ (pointList.get(0).getY() - pointList.get(
											pointList.size() - 1).getY())
									* (pointList.get(0).getY() - pointList.get(
											pointList.size() - 1).getY()));
							// System.out.println("dis"+distance);
							// System.out.println("disW"+distancewhole);
							int timewhole = Math.abs(timeend - timefirst);
							int timedistance = 0;
							// System.out.println(distancewhole+" "+timewhole+" "+distance);
							if (distancewhole != 0) {
								timedistance = (int) (distance * timewhole / distancewhole);
								// System.out.println("+++++++"+timedistance);
							} else {
								timedistance = 0;
							}
							// System.out.println("_______"+timedistance);
							// System.out.println("tf"+timefirst);
							// System.out.println("tn"+timeend);
							double middleX = Math.abs((firstX + endX) / 2);
							double middleY = Math.abs((firstY + endY) / 2);
							// timeInterval += timedistance;
							// int timesection = (timefirst+timeInterval)/3600;

							// System.out.println("timefirst"+timefirst);
							// System.out.println("timeend"+timeend);
							// if(timesection>86400){
							// System.out.println("timewhole:"+timewhole);
							// }
							// if(distancewhole<distance){
							// System.out.println(firstX+" "+endX+" "+firstY+" "+endY+" "+pointList.get(0).getX()+" "+pointList.get(0).getY()+" "+pointList.get(pointList.size()-1).getX()+" "+pointList.get(pointList.size()-1).getY());
							// }
							//
							double loninit = 113.758683;
							double latinit = 22.862324;
							double eachlon = 0.015683018;
							double eachlat = 0.014473207;

							// int gridNumber = (int)
							// ((Math.abs((Double.valueOf(lon)-lonf))/eachlon+1)+
							// ((int)(Math.abs((Double.valueOf(lat)-latf))/eachlat))*55);
							int gridNumberDistance = (int) ((Math.abs(middleY
									- loninit)
									/ eachlon + 1) + ((int) Math
									.abs((middleX - latinit)) / eachlat) * 55);
							// System.out.println("distance"+gridNumberDistance);
							// System.out.println("time"+timedistance);
							if (gridNumberDistance < 1596) {
								context.write(
										new Text(key),
										new Text(String
												.valueOf(gridNumberDistance)
												+ ","
												+ String.valueOf(timedistance)));
							}
						}
					}
				}

				lonf = lone;
				latf = late;
				timefirst = timeend;
				gridNumberf = gridNumbere;
			}

		}
	}

	// // map�׶ε����������map��List���з�����ÿ������ӳ�䵽һ��reducer
	// public static class StopExtrPartitioner extends Partitioner<Text, Text>{
	// @Override
	// public int getPartition(Text key, Text value, int numPartitions){
	// String[] s= key.toString().split("_");
	// String carplat = s[0].trim();//��map��õ���key�л�ó��ƺ�
	// //�ַ�����hashֵ�Ǹ����ַ�����ֵ����ģ���ֵͬ���ַ�������hashֵһ����ͬ
	// int id = carplat.hashCode();
	// int m = id%numPartitions;
	// return m;
	// }
	// }

	// ÿ���������ֵ���job.setSortComparatorClass����key�ıȽϺ�����������
	public static class DistinguishKeyComparator extends WritableComparator {
		protected DistinguishKeyComparator() {
			super(Text.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			String s1 = w1.toString();
			String s2 = w2.toString();
			int tmp = s1.split("_")[0].compareTo(s2.split("_")[0]);// �Ƚϳ��ƺ�
			// System.out.println("!!!!!!!!!!!!!!"+s1);
			if (0 == tmp) {
				// �����ƺ���ͬ
				try {
					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd HH:mm:ss");
					java.util.Date dt1;
					java.util.Date dt2;
					dt1 = sdf.parse(s1.split("_")[1]);
					dt2 = sdf.parse(s2.split("_")[1]);
					// �Ƚ�����
					return dt1.compareTo(dt2);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			return tmp;
		}
	}

	// ֻҪ����Ƚ����Ƚϵ�����key��ͬ�����Ǿ�����ͬһ����.
	// ���ǵ�value����һ��value���������������������keyʹ������ͬһ���������key�ĵ�һ��key
	public static class DistinguishKeyGroupComparator extends
			WritableComparator {
		protected DistinguishKeyGroupComparator() {
			super(Text.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			String s1 = w1.toString();
			String s2 = w2.toString();
			int tmp = s1.split("_")[0].compareTo(s2.split("_")[0]);// �Ƚϳ��ƺ�
			int hr = s1.split("_")[2].compareTo(s2.split("_")[2]);// �Ƚ�Сʱ
			int comp = 1;
			if (tmp == 0 && hr == 0) {
				comp = 0;
			}
			return comp;
		}
	}

	public int run(String[] args) throws Exception {
		// JarUtil.jar(DistinguishGridCarDriver.class.getClassLoader().getResource("").getFile(),
		// "DistinguishCarDriver.jar");
		// System.out.println(new File("DistinguishCarDriver.jar")
		// .getAbsolutePath());
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Test");
		job.setJarByClass(Test.class);
		// ((JobConf)
		// job.getConfiguration()).setJar("DistinguishCarDriver.jar");
		job.setMapperClass(DistinguishGridCarMapper.class);
		job.setReducerClass(DistinguishGridCarReducer.class);

		job.setNumReduceTasks(5);// ָ��reduce�ĸ�����Ĭ��Ϊ1

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// mapreduce�Ķ�������
		// ��������
		// job.setPartitionerClass(StopExtrPartitioner.class);
		job.setSortComparatorClass(DistinguishKeyComparator.class);
		// ���麯��
		job.setGroupingComparatorClass(DistinguishKeyGroupComparator.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

}

class point implements Comparable<point> {
	double x = 0.0;

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

	double y = 0.0;

	@Override
	public int compareTo(point p) {
		// TODO Auto-generated method stub
		return new Double(this.getX()).compareTo(new Double(p.getX()));
	}

}
