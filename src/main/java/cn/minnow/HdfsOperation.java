package cn.minnow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;

import cn.helium.kvstore.common.KvStoreConfig;

public class HdfsOperation {
	private Configuration conf = new Configuration();
	private static final String HDFS_PATH = KvStoreConfig.getHdfsUrl();
//	private static final String HDFS_PATH = "hdfs://localhost:9000";
	private static final int ONE_NODE = 66060288;
	
	HdfsOperation(){
		conf = new Configuration();
		conf.setBoolean("dfs.support.append", true);
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER");
		conf.set("dfs.client.block.write.replace-datanode-on-failure.enable","true");
			
	}
	
	public boolean createFile(String filePath) {
		boolean create = false;
		try {
			FileSystem fs = FileSystem.get(new URI(HDFS_PATH),conf);
			Path dst = new Path(filePath); 
			
			create = fs.createNewFile(dst);
		} catch (IOException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		return create;
	}
	
	public boolean isExists(String filePath) {
		boolean exists = false;
		try {
			FileSystem fs = FileSystem.get(new URI(HDFS_PATH),conf);
			Path dst = new Path(HDFS_PATH + filePath); 
			System.out.println("isExists: "+ filePath);
			
			exists = fs.exists(dst);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		return exists;
	}
	
	public int fileSize(String filePath) {
		FileSystem fs;
		int sz = 0;
		try {
			fs = FileSystem.get(new URI("HDFS_PATH"),conf);
			Path file = new Path(filePath);
			sz = (int) fs.getContentSummary(file).getLength() ;
			return sz;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sz;
	}
	
	
	
	public boolean mkdir(String dir) {
		if(StringUtils.isBlank(dir)) {
			System.out.println("the dir should not be empty!");
			return false;
		}
		Configuration conf = new Configuration();  
        try {
			FileSystem fs = FileSystem.get(URI.create(dir), conf);
			if (!fs.exists(new Path(dir))) {  
	            fs.mkdirs(new Path(dir));  
	        }  
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return true;
	}
	
	
	
	public String whichFile() {

		String dst = HDFS_PATH + "/data";
		mkdir(dst);
		int idx = 0;
		FileSystem fs;
		try {
			fs = FileSystem.get(URI.create(dst), conf);
			FileStatus fileList[] = fs.listStatus(new Path(dst));
			int size = fileList.length; 
			System.out.println("Sizesziesize: " + size); 
			
			if(size != 0) {
				
	    		String lastName = fileList[size-1].getPath().getName();
	        	idx = size ;
	        	
	        	System.out.println("lastname: " + lastName); 
	        	
	        	System.out.println("idx: " + idx); 
	    	}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	
		String filePath = "/data/data"+ idx;
		if(!isExists(filePath))
			createFile(filePath);
		System.out.println("create path in which file: " + filePath); 
		return filePath;
	}
	
	//hdfs上的索引备份，同样根据进程ID区分
	public String whichIndexFile(int kvpodId, String key) {
		String indexFile =    "/index" + kvpodId;
		mkdir(HDFS_PATH+ indexFile);
		System.out.println("mkdir success!");
		switch(Math.abs(key.hashCode()%3)) {
			case 0:
				indexFile = indexFile + "/index0";
				break;
			case 1:
				indexFile = indexFile + "/index1";
				break;
			case 2:
				indexFile = indexFile + "/index2";
			break;	
		}
		System.out.println("indexfile: "+ indexFile);
		if(!isExists(indexFile))
			createFile(indexFile);
		return indexFile;
	}
	
	public void append(String fileName, String line) {
		try {
			FileSystem fs = FileSystem.get(URI.create(HDFS_PATH + fileName), conf);
			FSDataOutputStream out = fs.append(new Path(HDFS_PATH + fileName)); 
//			int readLen = line.getBytes().length;  
//	        while (-1 != readLen) {  
//	            out.write(line.getBytes(), 0, readLen);  
//	            readLen--;
//	            System.out.println("line  byte length: "+ readLen + "\n");
//	        }  
			out.write(line.getBytes());  
			System.out.println("appending...");
			out.flush();
	        out.close();  
	        fs.close(); 
	        
	        System.out.println("append success!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	
	public String readHdfsIndex(String filePath,String key) {
		StringBuffer buffer = new StringBuffer();
		FSDataInputStream fsr = null;
		BufferedReader bufferedReader = null;
		String line = null;
		String index = "";
		
		try {
			FileSystem fs = FileSystem.get(URI.create(filePath),conf);
			fsr = fs.open(new Path(filePath));
			bufferedReader = new BufferedReader(new InputStreamReader(fsr));
			
			while ((line = bufferedReader.readLine()) != null)
			{
				if(key.equals(line.split(",")[0])) {
					index = line.split(",")[1];
				}
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			return null;
		}
		return index;
	}
	
	
	
	public Map<String,String> readHdfs(String filePath,String key) {
		
		FSDataInputStream fsr = null;
		BufferedReader bufferedReader = null;
		String line = null;
		Map<String,String> value = new HashMap<String,String>();
		
		try {
			FileSystem fs = FileSystem.get(URI.create(filePath),conf);
			fsr = fs.open(new Path(filePath));
			bufferedReader = new BufferedReader(new InputStreamReader(fsr));
			
			String jsonStr = "";

			while ((line = bufferedReader.readLine()) != null)
			{
				
				jsonStr += line;
//				String[] arr = line.split("\\{");
//				if(key.equals(arr[1].split("=")[0])){
//					String[] valueStr = (arr[2].split("\\}")[0]).split(",");
//					for(int i = 0; i < valueStr.length; i++) {
//						value.put(valueStr[i].split("=")[0], valueStr[i].split("=")[1]);
//					}
//				} 
				
			}
			
			System.out.println("jsonStr:" + jsonStr);
			Gson gson = new Gson();
			Map<String, Map<String,String>> map = gson.fromJson(jsonStr, HashMap.class);
			
			value = map.get(key);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return value;
		
	}
}
