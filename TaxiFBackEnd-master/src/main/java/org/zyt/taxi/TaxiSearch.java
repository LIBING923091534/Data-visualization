package org.zyt.taxi;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class TaxiSearch {

	public static ByteArrayOutputStream searchRoute(String startTime, String endTime, String carID, String startAreaString, String endAreaString, String maxView) throws IOException {

		List<GeoPoint> s1 = new ArrayList<GeoPoint>();
		List<GeoPoint> s2 = new ArrayList<GeoPoint>();
		if(startAreaString != "" && startAreaString != null){
			String list1[] = startAreaString.split(";");
			for (String pointStr : list1) {
				double x = Double.parseDouble(pointStr.split(",")[0]);
				double y = Double.parseDouble(pointStr.split(",")[1]);
				s1.add(new GeoPoint(x, y));
			}
		}else {
			s1 = null;
		}
		if(endAreaString != "" && endAreaString != null){
			String list2[] = endAreaString.split(";");
			for (String pointStr : list2) {
				double x = Double.parseDouble(pointStr.split(",")[0]);
				double y = Double.parseDouble(pointStr.split(",")[1]);
				s2.add(new GeoPoint(x, y));
			}
		}else {
			s2 = null;
		}
		

		
		List<String> resES = TaxiElasticSearch.searchIndexTaxiRoute(startTime, endTime, carID, s1, s2, maxView);
		List<String> resHb = new ArrayList<String>();
		
		// 字节流
		ByteArrayOutputStream baos=new ByteArrayOutputStream();
		
		TaxiHbase.init();
		for(String s : resES){
//			resHb.add(TaxiHbase.getData("taxi-route", s, "info", null));
			String listString[] = TaxiHbase.getData("taxi-route", s, "info", null).split(",");
			for(String item : listString){
				baos.write(getBytes(Float.floatToIntBits(Float.parseFloat(item))));
			}
		}
		TaxiHbase.close();

//		String resString = "{\"result\":" + resHb.toString() + "}";
		
		return baos;
	}
    public static byte[] getBytes(int data)  
    {  
        byte[] bytes = new byte[4];  
        bytes[0] = (byte) (data & 0xff);  
        bytes[1] = (byte) ((data & 0xff00) >> 8);  
        bytes[2] = (byte) ((data & 0xff0000) >> 16);  
        bytes[3] = (byte) ((data & 0xff000000) >> 24);  
        return bytes;  
    }  
	public static void main(String[] args) throws IOException{

		System.out.println(searchRoute("1391110839","1391110838",null,null,null,""));
	}
}
