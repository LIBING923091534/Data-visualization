package org.zyt.taxi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.zyt.taxi.tp.TpWritable;

import sun.tools.tree.ThisExpression;
import ch.hsr.geohash.GeoHash;

public class TaxiHbase {
	public static Configuration myConf;
	public static Connection myConn;
	public static Admin admin;
	public static String tableName1 = "taxi-route";
	public static String tableName2 = "taxi-point";
	public static Table table1;
	public static Table table2;
	
	public static void main(String[] args) throws IOException {
//		// 创建表
//		createTable("user", new String[]{"info"});
//		// 插入数据
//		insertRow("user", "TheRealMT", "info", "email", "samuel1@clemens.org");
//		insertRow("user", "TheRealMT", "info", "password", "Langhorne");
//		// 删除数据
////		deleteRow("user", "TheRealMT", "info", null);
//		// 获取数据
//		getData("user", "TheRealMT", "info",null);
//		// 删除表
//		deleteTable("user");
		insertRouteAndTp();
	}
	
	// 读取文件，插入轨迹表 和 轨迹点表
	public static void insertRouteAndTp () {
        try{
        	String path = "/taxi/output/";
            String fileName = "part-r-00000";
            String tableName1 = "taxi-route";
            String tableName2 = "taxi-point";
            String colFamily = "info";
            
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            Path file = new Path(path + fileName);
            if(fs.exists(file)){
                System.out.println("文件存在");
                // 创建表（已存在自动跳过）
                createTable(tableName1, new String[]{colFamily});
                createTable(tableName2, new String[]{colFamily});
               
                try {
                	// 读取HDFS中的指定文件
                    FSDataInputStream getIt = fs.open(file);
                    BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
                    // 打开与HBase的连接
                    init();
                    // 轨迹表 与 点表
                    Table table1 = myConn.getTable(TableName.valueOf(tableName1));
                    Table table2 = myConn.getTable(TableName.valueOf(tableName2));
                    String content = d.readLine(); //读取文件一行
                    while(content != null){
                    	String carID = content.split(";")[0].split(" ")[0];
                    	String startTime = content.split(";")[0].split(" ")[1];
                    	String endTime = content.split(";")[0].split(" ")[2];
                    	String routeID = MD5.getMD5(startTime+endTime+carID);
                    	String[] tps = content.split(";")[1].split(",");
                    	
                    	// 轨迹表行键组成：轨迹的起始时间+“ ”+结束时间+“ ”+车辆编号+“ ”+轨迹编号
                    	String rowkey = startTime.toString() + " " + endTime.toString() + " " + carID + " " + routeID;
                    	Put put1 = new Put(rowkey.getBytes());
						put1.addColumn(colFamily.getBytes(), "carID".getBytes(), carID.getBytes());
						for (int i = 0; i < tps.length ; i++) {
							String tp = tps[i];
							String lat = tp.split(" ")[0];
							String lng = tp.split(" ")[1];
							String timestamp = tp.split(" ")[2];
							
							// 轨迹表，一条轨迹一行，组成的每个轨迹点一列
							put1.addColumn(colFamily.getBytes(), (""+i).getBytes(), (lat+" "+lng+" "+timestamp).getBytes());
							
							// 轨迹点表， 一个轨迹点一行
							// 轨迹点表行键组成： GeoHash+“ ”+时间
							GeoHash pointHash = GeoHash.withCharacterPrecision(Integer.parseInt(lat)/1000000.0, Integer.parseInt(lng)/1000000.0, 11);
							String pointHashStr = pointHash.toBase32();
							Put put2 = new Put((pointHashStr+" "+timestamp).getBytes());
							put2.addColumn(colFamily.getBytes(), "lat".getBytes(), lat.getBytes());
							put2.addColumn(colFamily.getBytes(), "lng".getBytes(), lng.getBytes());
							put2.addColumn(colFamily.getBytes(), "carID".getBytes(), carID.getBytes());
							table2.put(put2);
						}
                		table1.put(put1);
                    	content = d.readLine();
                    }
            		table1.close();
            		table2.close();
            		close();
            		
                    d.close(); //关闭文件
                    fs.close(); //关闭hdfs
        		} catch (Exception e) {
        		        e.printStackTrace();
        		}
            }else{
                System.out.println("文件不存在");
            }
 
        }catch (Exception e){
            e.printStackTrace();
        }
	}

	public static void init(){
		myConf = HBaseConfiguration.create();
		myConf.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
		try{
			myConn = ConnectionFactory.createConnection(myConf);
			admin = myConn.getAdmin();
			table1 = myConn.getTable(TableName.valueOf(tableName1));
			table2 = myConn.getTable(TableName.valueOf(tableName2));
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public static void close(){
		try{
			if(table1 != null){
				table1.close();
			}
			if(table2 != null){
				table2.close();
			}
			if(admin != null){
				admin.close();
			}
			if(null != myConn){
				myConn.close();
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public static void insertIntoTable(String tb, Put put) throws IOException {
		if(tb == tableName1){
			table1.put(put);
		}else if(tb == tableName2){
			table2.put(put);
		}
	}
	
	// 创建表
	// 参数： 表名 列族
	public static void createTable(String tableName, String[] colFamily) throws IOException {
		init();
		TableName tName = TableName.valueOf(tableName);
		
		if(admin.tableExists(tName)){
			System.out.println("Table Already exists!");
		}else{
			HTableDescriptor hTableDescriptor = new HTableDescriptor(tName);
			for(String str:colFamily){
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
				hTableDescriptor.addFamily(hColumnDescriptor);
			}
			admin.createTable(hTableDescriptor);
			System.out.println("create table success");
		}
		
		close();
	}
	
	// 查找数据
	// 参数： 表名 行键 列族 列名
	public static String getData(String tableName, String rowkey, String colFamily,
			String col) throws IOException{

		Table table = myConn.getTable(TableName.valueOf(tableName));
		Get get = new Get(rowkey.getBytes());
		if(col == null){
			get.addFamily(colFamily.getBytes());
		}else {
			get.addColumn(colFamily.getBytes(), col.getBytes());
		}
		Result result = table.get(get);
		table.close();
		
		return formatShow2(result);
	}
	
	// 删除表
    public static void dropTable(String tableName) {  
        try {  
        	init();
            admin.disableTable(TableName.valueOf(tableName));  
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("Delete table success");
            close();
        } catch (MasterNotRunningException e) {  
            e.printStackTrace();  
        } catch (ZooKeeperConnectionException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        } finally{
        	close();
        	System.out.println("Delete table faild");
        }
  
    }  
	
    // 以json格式输出
	public static String formatShow(Result r) throws IOException{
		Cell[] cells = r.rawCells();

		XContentBuilder resJson = XContentFactory.jsonBuilder()
	            .startObject()
	            .startObject("info");
		HashMap<String, Object> res = new HashMap<String, Object>();
		for(Cell cell:cells){
			res.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
			resJson.field(new String(CellUtil.cloneQualifier(cell)),  new String(CellUtil.cloneValue(cell)));
            System.out.println("col:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
		}
		resJson.endObject().endObject();
		
		return resJson.string();
	}
	
	// 以csv格式输出
	public static String formatShow2(Result r) throws IOException{
		Cell[] cells = r.rawCells();

		HashMap<String, String> res = new HashMap<String, String>();
		for(Cell cell:cells){
			res.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
		}
		return res.get("startTime") + "," + res.get("endTime") + "," +  res.get("avgSpeed") + "," + res.get("afterLength") + "," + res.get("tpList"); 
	}
}
