package org.zyt.taxi;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import ch.hsr.geohash.GeoHash;

public class TaxiInsert {
	private static final String SEPARATOR = ",";
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
//                // 删除表、索引表
                TaxiElasticSearch.deleteIndex(tableName1);
                TaxiHbase.dropTable(tableName1);
//                TaxiHbase.dropTable(tableName2);
                // 创建表（已存在自动跳过）
                TaxiHbase.createTable(tableName1, new String[]{colFamily});
//                TaxiHbase.createTable(tableName2, new String[]{colFamily});
               // 创建索引表（已存在自动跳过）
                TaxiElasticSearch.createIndex(tableName1, colFamily, TaxiElasticSearch.getTaxiRouteMappingJson());
                
                try {
                	// 读取HDFS中的指定文件
                    FSDataInputStream getIt = fs.open(file);
                    BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
                    // 打开与HBase的连接,创建表连接
                    TaxiHbase.init();
                    // 获取ES的连接
					TaxiElasticSearch.getClient();
                    // 轨迹表 与 点表          
                    String content = d.readLine(); //读取文件一行
                    while(content != null){
                    	String carID = content.split(";")[0].split(" ")[0];
                    	String startTime = content.split(";")[0].split(" ")[1];
                    	String endTime = content.split(";")[0].split(" ")[2];
                    	String afterLength = content.split(";")[0].split(" ")[3];
                    	String avgSpeed = content.split(";")[0].split(" ")[4];
                    	String routeID = MD5.getMD5(startTime+endTime+carID);
                    	String[] tps = content.split(";")[1].split(",");
                    	double startPointX = geoLongToFloat(tps[0].split(" ")[0]);
                    	double startPointY = geoLongToFloat(tps[0].split(" ")[1]);
                    	double endPointX = geoLongToFloat(tps[tps.length - 1].split(" ")[0]);
                    	double endPointY = geoLongToFloat(tps[tps.length - 1].split(" ")[1]);
                    	
                    	String startPoint = startPointX  + "," + startPointY;
                    	String endPoint = endPointX + "," + endPointY;

                    	// 轨迹表行键组成：轨迹的起始时间+“ ”+结束时间+“ ”+车辆编号+“ ”+轨迹编号
//                    	String rowkey = startTime.toString() + " " + endTime.toString() + " " + carID + " " + routeID;
                    	// 轨迹表行键组成(加入ES后)：轨迹ID
                    	String rowkey = routeID;
                    	Put put1 = new Put(rowkey.getBytes());
						put1.addColumn(colFamily.getBytes(), "carID".getBytes(), carID.getBytes());
						put1.addColumn(colFamily.getBytes(), "startTime".getBytes(), startTime.getBytes());
						put1.addColumn(colFamily.getBytes(), "endTime".getBytes(), endTime.getBytes());
						put1.addColumn(colFamily.getBytes(), "afterLength".getBytes(), afterLength.getBytes());
						put1.addColumn(colFamily.getBytes(), "avgSpeed".getBytes(), avgSpeed.getBytes());
						put1.addColumn(colFamily.getBytes(), "startPoint".getBytes(), startPoint.getBytes());
						put1.addColumn(colFamily.getBytes(), "endPoint".getBytes(), endPoint.getBytes());
						
				        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()  
				                .startObject()
				                .field("startTime", startTime)  
				                .field("endTime", endTime)
				                .field("carID", carID)
				                .field("afterLength", afterLength)
				                .field("avgSpeed", avgSpeed)
				                .field("startPoint", startPoint)
				                .field("endPoint", endPoint)
				                .endObject(); 

				        // 向ES中添加索引
						TaxiElasticSearch.addIndex(tableName1, colFamily, routeID, jsonBuilder);
				        
						List<Float> ptList = new ArrayList<Float>();
						
						for (int i = 0; i < tps.length ; i++) {
							String tp = tps[i];
							String lat = tp.split(" ")[0];
							String lng = tp.split(" ")[1];
							String timestamp = tp.split(" ")[2];
							
							// 轨迹表，一条轨迹一行，轨迹拼凑成字符串，存成一列
							ptList.add(geoLongToFloat(lat));
							ptList.add(geoLongToFloat(lng));
							
//							put1.addColumn(colFamily.getBytes(), (""+i).getBytes(), (lat+" "+lng+" "+timestamp).getBytes());
							
							// 轨迹点表， 一个轨迹点一行
							// 轨迹点表行键组成： GeoHash+“ ”+时间
//							GeoHash pointHash = GeoHash.withCharacterPrecision(Integer.parseInt(lat)/1000000.0, Integer.parseInt(lng)/1000000.0, 11);
//							String pointHashStr = pointHash.toBase32();
//							Put put2 = new Put((pointHashStr+" "+timestamp).getBytes());
//							put2.addColumn(colFamily.getBytes(), "lat".getBytes(), lat.getBytes());
//							put2.addColumn(colFamily.getBytes(), "lng".getBytes(), lng.getBytes());
//							put2.addColumn(colFamily.getBytes(), "carID".getBytes(), carID.getBytes());
//							TaxiHbase.insertIntoTable(tableName2, put2);
						}
						
						StringBuilder csvBuilder = new StringBuilder();
						for(Float dd : ptList){
							csvBuilder.append(dd+"");
							csvBuilder.append(SEPARATOR);
						}
						String csv = tps.length + "," + csvBuilder.toString();
						csv = csv.substring(0, csv.length() - SEPARATOR.length());
						put1.addColumn(colFamily.getBytes(), "tpList".getBytes(), csv.getBytes());
						TaxiHbase.insertIntoTable(tableName1, put1);
                    	content = d.readLine();
                    }
            		TaxiHbase.close();
            		TaxiElasticSearch.closeClient();
            		
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
	
	public static float geoLongToFloat(String s) {
		return Integer.parseInt(s)/1000000.0f;
	}
}
