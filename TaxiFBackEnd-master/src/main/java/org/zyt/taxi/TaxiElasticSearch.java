package org.zyt.taxi;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.queryparser.xml.builders.TermQueryBuilder;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.CommonTermsQueryBuilder;
import org.elasticsearch.index.query.GeoPolygonQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.MultiPointBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;

public class TaxiElasticSearch {

	private static String host = "localhost";
	private static int port = 9300;
	private static TransportClient client = null;

	
	// 获取client
	public static void getClient() throws UnknownHostException{
		Settings settings = Settings.builder()
				.put("client.transport.sniff", true).build();

		client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName(host), port));
	}
	// 关闭client
	public static void closeClient() {
		if(client != null){
			client.close();
		}
	}
	
	// 创建index库
	public static void createIndex(String indexName, String type, XContentBuilder mapJson) throws IOException{
		getClient();
		CreateIndexRequestBuilder prepareCreate = client.admin().indices().prepareCreate(indexName);   
//        System.out.println(mapJson.string());
        prepareCreate.addMapping(type, mapJson);
        
        try{
	        CreateIndexResponse createResponse = prepareCreate.execute().actionGet();
	        System.out.println("Create index success!");
        }catch(ResourceAlreadyExistsException e){
        	System.out.println("Index already exists");
        }finally{
        	closeClient();
        }
	}
	
	// 删除index库
	public static void deleteIndex(String indexName) throws UnknownHostException {
		getClient();
		IndicesExistsRequest inExistsRequest = new IndicesExistsRequest(indexName);

		IndicesExistsResponse inExistsResponse = client.admin().indices()
		                    .exists(inExistsRequest).actionGet();
		if(inExistsResponse.isExists()){
			DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(indexName)
	                .execute().actionGet();
			if(dResponse.isAcknowledged()){
				System.out.println("Delete Succeed!");
			}else {
				System.out.println("Delete Faild!");
			}
		}else {
			System.out.println("Index not found!");
		}
		closeClient();
	}
	
	public static XContentBuilder getTaxiRouteMappingJson() throws IOException{
		XContentBuilder mapJson = XContentFactory.jsonBuilder()  
	            .startObject()
	            .startObject("info")
	            .startObject("properties")
	            .startObject("startTime").field("type", "date").endObject()
	            .startObject("endTime").field("type", "date").endObject()
	            .startObject("afterLength").field("type", "double").endObject()
	            .startObject("avgSpeed").field("type", "double").endObject()
	            .startObject("carID").field("type", "text").field("fielddata", true).endObject()
	            .startObject("startPoint")
	                .field("type", "geo_point").endObject()
	            .startObject("endPoint")
	            .field("type", "geo_point").endObject()
	                .endObject().endObject().endObject();
		return mapJson;
	}
	// 添加index记录
	public static void addIndex(String indexName, String typeName, String id, XContentBuilder json) throws IOException {
        // prepareIndex方法：索引数据到ElasticSearch
        IndexResponse response = client.prepareIndex(indexName,typeName,id)  
            .setSource(json)
            .get();
        RestStatus status = response.status();
	}
	
	// 按起始时间、终止时间、车辆编号、起始点位置、终止点位置查询轨迹
	public static List<String> searchIndexTaxiRoute(String startTime, String endTime, String carID, 
			List<GeoPoint> startPointArea, List<GeoPoint> endPointArea, String maxView) throws UnknownHostException{
		getClient();
		// 最大返回数
		int maxNum = 10000;
		if(maxView != null && maxView != ""){
			 maxNum = Integer.parseInt(maxView);
		}
		
		//时间范围的设定
        RangeQueryBuilder rangequerybuilder1 = QueryBuilders
                    .rangeQuery("startTime")
                    .to(endTime);
        
        RangeQueryBuilder rangequerybuilder2 = QueryBuilders
                .rangeQuery("endTime")
                .from(startTime);
        
        // 构造查询对象
        QueryBuilder qb = QueryBuilders.boolQuery()
        		.must(rangequerybuilder1)
        		.must(rangequerybuilder2);
        
        // 按照多边形范围查找点
//        startPointArea.add(new GeoPoint(30.608000, 114.280500));
//        startPointArea.add(new GeoPoint(30.608000, 114.298888));
//        startPointArea.add(new GeoPoint(30.613200, 114.298888));
//        startPointArea.add(new GeoPoint(30.613200, 114.280500));
//        endPointArea.add(new GeoPoint(0 , 0));
        
        if(carID != null && carID != ""){
        	// 查找指定车辆
            CommonTermsQueryBuilder matchQueryBuilder = new CommonTermsQueryBuilder("carID", carID);
        	qb = ((BoolQueryBuilder) qb).must(matchQueryBuilder);
        }
        if(startPointArea != null && startPointArea.size() >= 3){
            // 按照多边形范围查找起始点
            GeoPolygonQueryBuilder geoPolygonQueryBuilder1 = new GeoPolygonQueryBuilder(
                    "startPoint", startPointArea);
        	qb = ((BoolQueryBuilder) qb).must(geoPolygonQueryBuilder1);
        }
        if(endPointArea != null && endPointArea.size() >= 3){
        	// 按照多边形范围查找终止点
            GeoPolygonQueryBuilder geoPolygonQueryBuilder2 = new GeoPolygonQueryBuilder(
                    "endPoint", endPointArea);
        	qb = ((BoolQueryBuilder) qb).must(geoPolygonQueryBuilder2);
        }
        
        // 构造source，指定返回字段
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        sourceBuilder.fetchSource("startTime","");
        sourceBuilder.query(qb);

        //查询建立
        SearchRequestBuilder responsebuilder = client
                                .prepareSearch("taxi-route")
                                .setTypes("info");
        
        SearchResponse myresponse=responsebuilder
        			.setSource(sourceBuilder)
                    .setFrom(0).setSize(maxNum) //分页
                    .setExplain(true)
                    .execute()
                    .actionGet();
        
        SearchHits hits = myresponse.getHits();
        ArrayList<String> res= new ArrayList<String>();
        
        for(int i = 0; i < hits.getHits().length; i++) {
//            System.out.println(hits.getHits()[i].getSourceAsString());
        	res.add(hits.getHits()[i].getId());
        }
        closeClient();
        return res;
	}
	
	// 聚合，获取车辆数，总数，起始终止时间等
	public static String getCarNum() throws UnknownHostException {
		int carNum = 0;
		String minmaxDate = "";
		getClient();
		SearchRequestBuilder sbuilder = client.prepareSearch("taxi-route").setTypes("info");
		
		// terms聚合，获得carID个数
		AggregationBuilder teamAgg= AggregationBuilders.terms("carID_count").field("carID");
		sbuilder.addAggregation(teamAgg);
		SearchResponse response = sbuilder.execute().actionGet();
        Terms agg = response.getAggregations().get("carID_count"); 
        carNum = agg.getBuckets().size();

		// 统计记录总数
        SearchHits hits = response.getHits();
        
        // stats聚合，获得日期的最大最小值
		teamAgg= AggregationBuilders.stats("minDate_count").field("startTime");
		sbuilder.addAggregation(teamAgg);
		SearchResponse response2 = sbuilder.execute().actionGet();
		minmaxDate = response2.getAggregations().get("minDate_count").toString();

        closeClient();	
		return "{\"totalNum\":" + hits.totalHits + ", \"carNum\":" + carNum + ", \"dateInfo\":" + minmaxDate  + "}";
	}
	
	public static void main(String[] args) throws UnknownHostException {
		List<String> reStrings = searchIndexTaxiRoute(null,null,"",null,null,"");
		System.out.println(reStrings.size());
		 reStrings = searchIndexTaxiRoute("1391110839","1391110838","MMC8000GPSANDASYN051113-30346-00000000",null, null,"");
System.out.println(reStrings);
	}
	// 
}
