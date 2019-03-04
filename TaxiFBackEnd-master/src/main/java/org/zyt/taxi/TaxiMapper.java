package org.zyt.taxi;

import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.zyt.taxi.tp.TpWritable;


public class TaxiMapper extends Mapper<LongWritable, Text, Text, TpWritable>{

	private TpWritable tp = new TpWritable();
	private Text id = new Text();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, TpWritable>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		
		String[] fields = line.split(" ");

		// 经度为0,说明为异常数据，pass
		if (Long.parseLong(fields[2])==0){
			return;
		}
		
		// 每行12个字段
		if(fields.length==12){
			// 构造轨迹点实例
			tp.setId(new Text(fields[0]))
			.setTimestamp(Long.parseLong(fields[1]))
			.setLng(Long.parseLong(fields[2]))
			.setLat(Long.parseLong(fields[3]))
			.setStatus(isInArray(fields[7]));
			
			// key是 车辆编号+空格+记录时间
			id.set(fields[0]+" "+fields[1]);
		}else {
			return;
		}
		
		context.write(id, tp);
	}
	
	// 262144，262145两个状态表示载客，其他表示空载
	private int isInArray(String s){
		String[] arr = {"262144","262145"};
		for (String a : arr) {
			if(a.equals(s)){
				return 1;
			}
		}
		return 0;
	}
	
	// 分组时的比较依据，
	public static class MyGroupingComparator implements RawComparator<Text>{

		// 对象比较
		@Override
		public int compare(Text o1, Text o2) {
			String s1 = o1.toString();
			String s2 = o2.toString();
			
			return s1.split(" ")[0].compareTo(s2.split(" ")[0]);
		}

		// 字节流比较，arg0和arg3是序列化后的字节数组，arg1和arg4是起始位置，arg2和arg5数组长度
		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
				int arg4, int arg5) {
			
			// 只比较key的前39位，即车辆编号
			return WritableComparator.compareBytes(arg0, arg1, 39, arg3, arg4, 39);
		}

	}
}


