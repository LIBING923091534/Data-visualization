package org.zyt.taxi;

import java.awt.print.Printable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.zyt.taxi.tp.TpWritable;

import com.graphhopper.GraphHopper;
import com.graphhopper.PathWrapper;
import com.graphhopper.matching.GPXFile;
import com.graphhopper.matching.MapMatching;
import com.graphhopper.matching.MatchResult;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.routing.AlgorithmOptions;
import com.graphhopper.routing.Path;
import com.graphhopper.routing.util.FlagEncoder;
import com.graphhopper.routing.util.HintsMap;
import com.graphhopper.routing.weighting.FastestWeighting;
import com.graphhopper.util.CmdArgs;
import com.graphhopper.util.DistanceCalc;
import com.graphhopper.util.GPXEntry;
import com.graphhopper.util.Helper;
import com.graphhopper.util.InstructionList;
import com.graphhopper.util.Parameters;
import com.graphhopper.util.PathMerger;
import com.graphhopper.util.StopWatch;
import com.graphhopper.util.Translation;
import com.graphhopper.util.TranslationMap;

public class TaxiTableReducer extends TableReducer<Text, TpWritable, NullWritable>{
	private Text result = new Text();
	private NullWritable n = NullWritable.get();
	@Override
	protected void reduce(Text key, Iterable<TpWritable> values,
			Reducer<Text, TpWritable, NullWritable, Mutation>.Context context)
			throws IOException, InterruptedException {
		String s = new String();
		String colFamily = "info";
		
		int flag = 0;
		ArrayList<TpWritable> tpList = new ArrayList<>();
		
		// values包含同一辆车的轨迹点数据
		// flag 0 起始状态，未找到连续的载客状态轨迹点
		// flag 1 轨迹点初始状态，找到1个载客状态轨迹点
		// flag 2 连续匹配状态，找到大于1个载客状态轨迹点
		for (TpWritable tp : values) {
			if(flag == 0){
				if (tp.getStatus() == 1) {
					tpList.add(new TpWritable(tp.getId(), tp.getTimestamp(), tp.getLng(), tp.getLat(), tp.getStatus()));
					flag = 1;
				}else {
					continue;
				}
			}else if (flag == 1) {
				if (tp.getStatus() == 1) {
					// 找到第二个载客轨迹点，设置状态为2
					tpList.add(new TpWritable(tp.getId(), tp.getTimestamp(), tp.getLng(), tp.getLat(), tp.getStatus()));
					flag = 2;
				}else {
					// 只找到一个载客轨迹点，无效，清空数组
					tpList.clear();
					flag = 0;
				}
			}else if (flag == 2) {
				if (tp.getStatus() == 1) {
					tpList.add(new TpWritable(tp.getId(), tp.getTimestamp(), tp.getLng(), tp.getLat(), tp.getStatus()));
				}else {
					// 状态2遇到非载客点，此时数组中存在大于1个轨迹点，判定为1条轨迹
					String item = new String();
					
					try {
						// 地图匹配
						List<TpWritable> matchedTp = new RouteMatching().startMatching(tpList, ""+(int)(Math.random()*100000));
						// 按匀速计算各个点的时间戳
						Long startTime = tpList.get(0).getTimestamp();
						Long endTime = tpList.get(tpList.size()-1).getTimestamp();
						double interval = (endTime-startTime)*1.0/(matchedTp.size()-1);
				
						String rowkey = startTime.toString() + " " + endTime.toString() + " " + MD5.getMD5(key.toString());
						Put put = new Put(rowkey.getBytes());
						put.addColumn(colFamily.getBytes(), "carID".getBytes(), key.toString().split(" ")[0].getBytes());
						for (int i = 0; i < matchedTp.size(); i++) {
							TpWritable t = matchedTp.get(i);
							// 每个轨迹点一列
							put.addColumn(colFamily.getBytes(), (""+i).getBytes(), (t.getLat()+" "+t.getLng()+" "+Math.round(startTime+interval*i)).getBytes());
						}
						context.write(NullWritable.get(), put);
					} catch (Exception e) {
						// 在地图匹配中若发生错误（轨迹点太少、误差太大）则放弃这条轨迹
					}
					tpList.clear();
					flag = 0;
				}
			}
			
//			if(tp.getStatus() == 1){
//				s += tp.getLat()+" "+tp.getLng()+" "+tp.getTimestamp()+" "+tp.getStatus()+" ";
//			}
			
//			s += tp.getTimestamp()+","+tp.getStatus()+" ";
			
		}
		
//		result.set(key.toString()+","+s);
//		
//		context.write(result, n);
	}
	
	// 结果输出到本地文件中，方便检查匹配效果
	public static class LogInfo{  
        public static String LogFile="/home/hadoop/LogInfo";
        public static String Filepath = "/home/hadoop/route/";
        static{  
              
        }  
        public static void Begin(String region,String taskID){  
            File log=new File(LogFile);  
            FileOutputStream out; 
            try{  
                out=new FileOutputStream(LogFile, true);  
                out.write((region+" "+taskID+" begin/n").getBytes()); 
            }catch(FileNotFoundException e){  
                  
            }  
            catch(IOException e){  
                  
            }  
        }  
        public static void End(String region,String taskID){  
            //File log=new File(LogFile);  
            FileOutputStream out;  
            try{  
                out=new FileOutputStream(LogFile, true);  
                out.write((region+" "+taskID+" end/n").getBytes());  
            }catch(FileNotFoundException e){  
                  
            }  
            catch(IOException e){  
                  
            }
        }
        
        public static void write(String content, String fileName){
			File file = new File(Filepath+fileName);
			FileOutputStream outputStream;
			try {
				outputStream = new FileOutputStream(file, true);
				outputStream.write(content.getBytes());
			} catch (Exception e) {
				System.out.println("output error");
			}
		}
    }
	
	
	// 地图匹配算法
	public static class RouteMatching{
		private	String[] setings = {"action=match","gpx=/home/hadoop/taxi/map-matching-0.9/route/route8.gpx","graph.location=/home/hadoop/taxi/map-matching-0.9/graph-cache"};
		private CmdArgs args = CmdArgs.read(setings);
		private String grahpPath = "/home/hadoop/taxi/map-matching-0.9/graph-cache";
		
		private final Logger logger = LoggerFactory.getLogger(getClass());
		
		// random是为了区别在本地输出的文件
		public List<TpWritable> startMatching(ArrayList<TpWritable> tpList, String random) throws Exception {
			System.out.println(args);
			GraphHopper hopper = new GraphHopperOSM().init(args);
	        hopper.getCHFactoryDecorator().setEnabled(false);
	        
	        logger.info("loading graph from cache");
	        hopper.load(grahpPath);
	        
	        FlagEncoder firstEncoder = hopper.getEncodingManager().fetchEdgeEncoders().get(0);

	        int gpsAccuracy = args.getInt("gps_accuracy", -1);
	        if (gpsAccuracy < 0) {
	            // backward compatibility since 0.8
	            gpsAccuracy = args.getInt("gpx_accuracy", 50);
	        }

	        String instructions = args.get("instructions", "");
	        logger.info("Setup lookup index. Accuracy filter is at " + gpsAccuracy + "m");
	        AlgorithmOptions opts = AlgorithmOptions.start().
	                algorithm(Parameters.Algorithms.DIJKSTRA_BI).traversalMode(hopper.getTraversalMode()).
	                weighting(new FastestWeighting(firstEncoder)).
	                maxVisitedNodes(args.getInt("max_visited_nodes", 1000)).
	                // Penalizing inner-link U-turns only works with fastest weighting, since
	                // shortest weighting does not apply penalties to unfavored virtual edges.
	                hints(new HintsMap().put("weighting", "fastest").put("vehicle", firstEncoder.toString())).
	                build();
	        MapMatching mapMatching = new MapMatching(hopper, opts);
	        mapMatching.setTransitionProbabilityBeta(args.getDouble
	                ("transition_probability_beta", 2.0));
	        mapMatching.setMeasurementErrorSigma(gpsAccuracy);
	        
	        StopWatch importSW = new StopWatch();
	        StopWatch matchSW = new StopWatch();

	        Translation tr = new TranslationMap().doImport().get(instructions);

	        try {
	        	importSW.start();
//                List<GPXEntry> inputGPXEntries = new GPXFile().doImport(gpxFile.getAbsolutePath()).getEntries();
	        	// TpWritable转化为Entries
                List<GPXEntry> inputGPXEntries = this.getEntriesFromTpWritable(tpList, random);
                importSW.stop();
                matchSW.start();
                MatchResult mr = mapMatching.doWork(inputGPXEntries);
                matchSW.stop();

                System.out.println("\tmatches:\t" + mr.getEdgeMatches().size() + ", gps entries:" + inputGPXEntries.size());
                System.out.println("\tgpx length:\t" + (float) mr.getGpxEntriesLength() + " vs " + (float) mr.getMatchLength());
                System.out.println("\tgpx time:\t" + mr.getGpxEntriesMillis() / 1000f + " vs " + mr.getMatchMillis() / 1000f);

                InstructionList il;
                if (instructions.isEmpty()) {
                    il = new InstructionList(null);
                } else {
                    PathWrapper matchGHRsp = new PathWrapper();
                    Path path = mapMatching.calcPath(mr);
                    new PathMerger().doWork(matchGHRsp, Collections.singletonList(path), tr);
                    il = matchGHRsp.getInstructions();
                }
                
                return getTpWritableFromEntries(new GPXFile(mr, il).getEntries(),random);
			} catch (IllegalArgumentException ex) {
				importSW.stop();
                matchSW.stop();
//                logger.error("Problem with file " + "0"+ " Error: " + ex.getMessage(), ex);
                throw new Exception("error");
			}
		}

		// TpWritable转化为Entries
		private List<GPXEntry> getEntriesFromTpWritable(ArrayList<TpWritable> tpList, String random){
			List<GPXEntry> entries = new ArrayList<GPXEntry>();
			double defaultSpeed = 20;
			
			DistanceCalc distCalc = Helper.DIST_PLANE;
			double prevLat = 0, prevLon = 0;
            long prevMillis = 0;
            for (int index = 0; index < tpList.size(); index++) {

                double lat = tpList.get(index).getLat() / 1000000.0;
                double lon = tpList.get(index).getLng() / 1000000.0;
                long millis = prevMillis;
                if (tpList.get(index).getTimestamp() == 0) {
                    if (index > 0) {
                        millis += Math.round(distCalc.calcDist(prevLat, prevLon, lat, lon) * 3600 / defaultSpeed);
                    }
                } else {
                	millis = tpList.get(index).getTimestamp() * 1000;
                }

                entries.add(new GPXEntry(lat, lon, millis));
                // 30.607928 114.298521 1391151888000
                prevLat = lat;
                prevLon = lon;
                prevMillis = millis;
            }
//			输出GPX文件
//          String gpxContent = new GPXFile(entries).createString();
//			LogInfo.write(gpxContent, "m_"+random+"_a");
            return entries;
		}
		
		// Entries转化为TpWritable
		private List<TpWritable> getTpWritableFromEntries(List<GPXEntry> entries, String random){
//			输出GPX文件
//			String gpxContent = new GPXFile(entries).createString();
//			LogInfo.write(gpxContent, "m_"+random+"_b");
			
			List<TpWritable> tpList = new ArrayList<TpWritable>();
			Text empty = new Text("");
			for (GPXEntry entry : entries) {
				tpList.add(new TpWritable(empty,entry.getTime()/1000,Math.round(entry.getLon()*1000000),Math.round(entry.getLat()*1000000),1));
	        }
			
			return tpList;
		}
	}
}

