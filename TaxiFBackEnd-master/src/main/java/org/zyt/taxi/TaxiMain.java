package org.zyt.taxi;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.zyt.taxi.TaxiMapper.MyGroupingComparator;
import org.zyt.taxi.tp.TpWritable;

public class TaxiMain {
	public static void main(String[] args) throws Exception {
		TaxiMain.handleRoute(args);
	}
	
	public static boolean handleRoute(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		if(args.length < 2){
			args = new String[]{
					"hdfs://localhost:9000/taxi/input",
					"hdfs://localhost:9000/taxi/output"
			};
		}
		try {
			// 删除output中已有的数据
			FileSystem fileSystem = FileSystem.get(new URI(args[0]), new Configuration());  
			 if (fileSystem.exists(new Path(args[0]))) {  
			      fileSystem.delete(new Path(args[1]), true);  
			  }
			
//			String TableName = "taxi-route";
//			Configuration conf = HBaseConfiguration.create();
//			conf.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
////	        conf.set("hbase.zookeeper.quorum", "10.133.253.130");
////	        conf.set("hbase.zookeeper.property.clientPort", "2181");
////	        conf.set(TableOutputFormat.OUTPUT_TABLE, TableName);
	//
//			Job job = Job.getInstance(conf, "taxi");
	//
//			job.setJarByClass(TaxiMain.class);
//			job.setMapperClass(TaxiMapper.class);
//			job.setReducerClass(TaxiTableReducer.class);
	//
////			job.setNumReduceTasks(1);
//			// 设置分组依据
//			job.setGroupingComparatorClass(MyGroupingComparator.class);
//			
//			FileInputFormat.setInputPaths(job, new Path(args[0]));
//			
////			TableMapReduceUtil.addDependencyJars(job); 
//			TableMapReduceUtil.initTableReducerJob(TableName, TaxiTableReducer.class, job);
	//
//			job.setMapOutputKeyClass(Text.class);
//			job.setMapOutputValueClass(TpWritable.class);
	//
//	        job.setInputFormatClass(TextInputFormat.class);  
//	        job.setOutputFormatClass(TableOutputFormat.class);  
	       
			 
			// 直接输出文件
			Configuration conf = new Configuration();
	        
			Job job = Job.getInstance(conf, "taxi");

			job.setJarByClass(TaxiMain.class);
			job.setMapperClass(TaxiMapper.class);
			job.setReducerClass(TaxiReducer.class);

			// 设置分组依据
			job.setGroupingComparatorClass(MyGroupingComparator.class);
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(TpWritable.class);

//	        job.setInputFormatClass(TextInputFormat.class);  
//	        job.setOutputFormatClass(.class);  
			// Reduce 输出key，value类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
	       
			boolean isok = job.waitForCompletion(true);
			
			if(!isok){
				System.out.print("Failed!!");
				return false;
			}else{
				TaxiInsert.insertRouteAndTp();
			}
		} catch (Exception e) {
			return false;
		}
		return true;
	}
}
