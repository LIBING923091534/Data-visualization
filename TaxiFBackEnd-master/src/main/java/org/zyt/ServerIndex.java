package org.zyt;

import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.P;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload; 
import org.zyt.taxi.TaxiElasticSearch;
import org.zyt.taxi.TaxiMain;
import org.zyt.taxi.TaxiSearch;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

/**
 * Servlet implementation class ServerIndex
 */
public class ServerIndex extends HttpServlet {
	private static final long serialVersionUID = 1L;
    // 上传文件存储目录
    private static final String UPLOAD_DIRECTORY = "upload";
  
    // 上传配置
    private static final int MEMORY_THRESHOLD   = 1024 * 1024 * 3;  // 3MB
//    private static final int MAX_FILE_SIZE      = 1024 * 1024 * 4000; // 4000MB
//    private static final int MAX_REQUEST_SIZE   = 1024 * 1024 * 5000; // 5000MB
    private static final int MAX_FILE_SIZE      = -1; // 4000MB
    private static final int MAX_REQUEST_SIZE   = -1; // 5000MB
    /**
     * @see HttpServlet#HttpServlet()
     */
    public ServerIndex() {
        super();
        // TODO Auto-generated constructor stub
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
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
//        //得到请求的参数Map，注意map的value是String数组类型  
//        Map map = request.getParameterMap();  
//        Set<String> keySet = map.keySet();  
//        for (String key : keySet) {  
//           String[] values = (String[]) map.get(key);  
//           for (String value : values) {  
//               System.out.println(key+"="+value);  
//           }
//        }
//        ByteArrayOutputStream baos =  TaxiSearch.searchRoute("1391110839","1391110838", null, null, null);
//		ServletOutputStream out=response.getOutputStream();
//		out.write(baos.toByteArray());
        
//		response.setContentType("application/octet-stream;charset=UTF-8");
//		ByteArrayOutputStream baos=new ByteArrayOutputStream();
//        int intBits = Float.floatToIntBits(-74f);
//        int intBits2 = Float.floatToIntBits(40f);
//		baos.write(getBytes(intBits));
//		baos.write(getBytes(intBits2));
		
		PrintWriter out = response.getWriter();
		out.write(TaxiElasticSearch.getCarNum());
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
//		String string = request.getParameter("params");
//		Type t  = new TypeToken<List<Map<String,String>>>(){}.getType();
//		List<Map<String,String>> list = new Gson().fromJson(request.getParameter("params"), t);
//		for(Map<String,String> map : list){
//		    System.out.print("pk:"+map.get("st"));
//		    System.out.println("\tname:"+map.get("et"));
//		}
		
		try {
			/* 设置响应头允许ajax跨域访问 */
	        response.setHeader("Access-Control-Allow-Origin", "*");  
	        /* 星号表示所有的异域请求都可以接受， */  
	        response.setHeader("Access-Control-Allow-Methods", "GET,POST");  
	        
	        // 检测是否为多媒体上传
	        if (ServletFileUpload.isMultipartContent(request)) {
	        	// 配置上传参数
	        	
		        DiskFileItemFactory factory = new DiskFileItemFactory();
		        // 设置内存临界值 - 超过后将产生临时文件并存储于临时目录中
		        factory.setSizeThreshold(MEMORY_THRESHOLD);
		        // 设置临时存储目录
		        factory.setRepository(new File(System.getProperty("java.io.tmpdir")));
		  
		        ServletFileUpload upload = new ServletFileUpload(factory);
		          
		        // 设置最大文件上传值
		        upload.setFileSizeMax(MAX_FILE_SIZE);
		          
		        // 设置最大请求值 (包含文件和表单数据)
		        upload.setSizeMax(MAX_REQUEST_SIZE);
		  
		        // 构造临时路径来存储上传的文件
		        // 这个路径相对当前应用的目录
		        String uploadPath = getServletContext().getRealPath("./") + File.separator + UPLOAD_DIRECTORY;
		        
		          
		        // 如果目录不存在则创建
		        File uploadDir = new File(uploadPath);
		        if (!uploadDir.exists()) {
		            uploadDir.mkdir();
		        }
		  
		        try {
		            // 解析请求的内容提取文件数据
		            @SuppressWarnings("unchecked")
		            List<FileItem> formItems = upload.parseRequest(request);
		            if (formItems != null && formItems.size() > 0) {
		                // 迭代表单数据
		                for (FileItem item : formItems) {
		                    // 处理不在表单中的字段
		                    if (!item.isFormField()) {
		                    	
		                    	if(ServerIndex.saveToHDFS("test",item.get())){
			    		            PrintWriter writer = response.getWriter();
			    		            writer.println("Success");
			    		            writer.flush();
			    		            return;
		                    	}else{
		                    		PrintWriter writer = response.getWriter();
			    		            writer.println("Failed");
			    		            writer.flush();
			    		            return;
		                    	}
//		                        String fileName = new File(item.getName()).getName();
//		                        String filePath = uploadPath + File.separator + fileName;
//		                        File storeFile = new File(filePath);
//		                        // 在控制台输出文件的上传路径
//		                        //System.out.println(filePath);
//		                        // 保存文件到硬盘
//		                        item.write(storeFile);
//		                        request.setAttribute("message",
//		                            "文件上传成功!");

		                    }
		                }
		            }
		        } catch (Exception ex) {
		            request.setAttribute("message",
		                    "错误信息: " + ex.getMessage());
		            PrintWriter writer = response.getWriter();
		            writer.println("Error: "+ex.getMessage());
		            writer.flush();
		            return;
		        }
	        }
	  
	        
	        
	        
	        
	        
			Type t  = new TypeToken<Map<String,String>>(){}.getType();
			Map<String,String> map = new Gson().fromJson(request.getParameter("params"), t);

			
			String maxView = map.get("maxView");
			String st = map.get("st");
			String et = map.get("et");
			String carID = map.get("carID");
			String area1 = map.get("area1");
			String area2 = map.get("area2");
			String area3 = map.get("area3");
			String startArea = "";
			String endArea = "";
			if(area1 != null && area1 != ""){
				startArea = area1;
				endArea = area1;
			}
			if(area2 != null && area2 != ""){
				startArea = area2;
			}
			if(area3 != null && area3 != ""){
				endArea = area3;
			}
			
	        ByteArrayOutputStream baos =  TaxiSearch.searchRoute(st,et, carID, startArea, endArea, maxView);
			ServletOutputStream out=response.getOutputStream();
			out.write(baos.toByteArray());
		} catch (Exception e) {

			PrintWriter pw = response.getWriter();
			e.printStackTrace(pw);
		}
	}

	public static boolean saveToHDFS(String filename, byte[] buff) { 
        try {
                Configuration conf = new Configuration();  
                conf.set("fs.defaultFS","hdfs://localhost:9000");
                conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
                FileSystem fs = FileSystem.get(conf);
                filename = "hdfs://localhost:9000/taxi/input/"+filename;
                if(fs.exists(new Path(filename))){
                    fs.delete(new Path(filename), true);
                }
                FSDataOutputStream os = fs.create(new Path(filename));
                os.write(buff,0,buff.length);
                System.out.println("Create:"+ filename);
                os.close();
                fs.close();
                // 开始处理
                return TaxiMain.handleRoute(new String[0]);
                
        } catch (Exception e) {  
                e.printStackTrace(); 
                return false;
        }  
}  
}
