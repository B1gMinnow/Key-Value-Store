package kvstore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsOperation {
	private Configuration conf = new Configuration();
	private static final String HDFS_PATH = "hdfs://localhost:9000";
	private static final int ONE_NODE = 66060288;
	
	HdfsOperation(){
		System.out.println("new a hdfs operation!");
	}
	
	public boolean createFile(String filePath) {
		boolean create = false;
		try {
			FileSystem fs = FileSystem.get(conf);
			Path dst = new Path(HDFS_PATH + filePath); 
			
			create = fs.createNewFile(dst);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		return create;
	}
	
	public boolean isExists(String filePath) {
		boolean exists = false;
		try {
			FileSystem fs = FileSystem.get(conf);
			Path dst = new Path(HDFS_PATH + filePath); 
			
			exists = fs.exists(dst);
		} catch (IOException e) {
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
	
	public String whichFile(int kvpodId) {
		//加入进程ID防止写冲突
		String dst = HDFS_PATH + "/node" + kvpodId;
		String fileName = "";
		
		try {
			FileSystem fs = FileSystem.get(URI.create(dst), conf);  
			FileStatus fileList[] = fs.listStatus(new Path(dst));
			int size = fileList.length;  
	        for (int i = 0; i < size; i++) {  
//	            System.out.println("name:" + fileList[i].getPath().getName()  
//	                    + "\t\tsize:" + fileList[i].getLen()); 
	            if(fileList[i].getLen() < ONE_NODE) {
	            	fileName = fileList[i].getPath().getName();
	            	break;
	            }
	        } 
	        if(fileName == null || fileName == "") {
	        	String lastName = fileList[size-1].getPath().getName();
	        	int idx = Integer.parseInt(lastName.substring(4)) + 1;
	        	fileName = "data" + idx;
	        	createFile("/node" + kvpodId + "/" + fileName);
	        }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String filePath = "/node" + kvpodId + "/" + fileName;
		return filePath;
	}
	
	//hdfs上的索引备份，同样根据进程ID区分
	public String whichIndexFile(int kvpodId, String key) {
		String indexFile =  HDFS_PATH + "/index" + kvpodId;
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
		if(!isExists(indexFile))
			createFile(indexFile);
		return indexFile;
	}
	
	public void append(String fileName, String line) {
		try {
			FileSystem fs = FileSystem.get(URI.create(HDFS_PATH + fileName), conf);
			FSDataOutputStream out = fs.append(new Path(HDFS_PATH + fileName)); 
			int readLen = line.getBytes().length;  
	        while (-1 != readLen) {  
	            out.write(line.getBytes(), 0, readLen);  
	        }  
	        out.close();  
	        fs.close(); 
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
		
		StringBuffer buffer = new StringBuffer();
		FSDataInputStream fsr = null;
		BufferedReader bufferedReader = null;
		String line = null;
		Map<String,String> value = new HashMap<String,String>();
		
		try {
			FileSystem fs = FileSystem.get(URI.create(filePath),conf);
			fsr = fs.open(new Path(filePath));
			bufferedReader = new BufferedReader(new InputStreamReader(fsr));
			
			while ((line = bufferedReader.readLine()) != null)
			{
				String[] arr = line.split("\\{");
				if(key.equals(arr[1].split("=")[0])){
					String[] valueStr = (arr[2].split("\\}")[0]).split(",");
					for(int i = 0; i < valueStr.length; i++) {
						value.put(valueStr[i].split("=")[0], valueStr[i].split("=")[1]);
					}
				} 
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return value;
		
	}
}
