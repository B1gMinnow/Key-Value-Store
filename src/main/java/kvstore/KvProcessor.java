package kvstore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;

import cn.helium.kvstore.processor.Processor;
import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.rpc.*;


public class KvProcessor implements Processor {

	private Configuration conf = new Configuration();
	static final String HDFS_PATH = "hdfs://localhost:9000";
	static final int ONE_NODE = 134217728;
	
	public static void main(String args[]) {
//		KvStoreConfig config = new KvStoreConfig();
//		System.out.println("url:" + config.getServersNum());
		

		System.out.println("success!");
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
	
	public void writeToLocal(String filePath,String line) {
		FileWriter writer = null; 
		try {
			File file = new File(filePath);
			if(!file.exists()) {
				file.createNewFile();
			}
			writer = new FileWriter(filePath, true);
			writer.write(line); 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(writer != null){  
                try {
					writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}     
            } 
		}
		
	}
	
	public String readIndex(String filePath,String key) {
		File file = new File(filePath);
        BufferedReader reader = null;
        String res = "";
        try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
            
            while ((tempString = reader.readLine()) != null) {
                
                String[] line =  tempString.split(",");
                if(key.equals(line[0])) {
                	res = line[1];
                }
            }
            reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return res;
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
	

	@Override
	public boolean batchPut(Map<String, Map<String, String>> arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int count(Map<String, String> arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Map<String, String> get(String key) {
		// TODO Auto-generated method stub
		
		String indexFile = "";
		switch(Math.abs(key.hashCode()%3)) {
		case 0:
			indexFile = "/index0";
			break;
		case 1:
			indexFile = "/index1";
			break;
		case 2:
			indexFile = "/index2";
			break;	
		}
		String filePath = readIndex("D:/cloudindex" + indexFile,key);
		
		Map<String,String> value = readHdfs(HDFS_PATH + filePath,key);
		
		
		return value;
	}

	@Override
	public Map<Map<String, String>, Integer> groupBy(List<String> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] process(byte[] arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean put(String key, Map<String, String> value) {
		// TODO Auto-generated method stub
		int kvpodId = RpcServer.getRpcServerId();
		
//		if(!isExists("/index/index1")) {
//			createFile("/index/index1");
//		}
//		if(!isExists("/index/index2")) {
//			createFile("/index/index2");
//		}
//		if(!isExists("/index/index3")) {
//			createFile("/index/index3");
//		}
		String filePath = whichFile(kvpodId);
		String indexFile = "";
		switch(Math.abs(key.hashCode()%3)) {
			case 0:
				indexFile = "/index0";
				break;
			case 1:
				indexFile = "/index1";
				break;
			case 2:
				indexFile = "/index2";
				break;	
		}
		
		String index = key + "," + filePath + "\n";
		writeToLocal("D:/cloudindex"+indexFile,index);
		
		//append(indexFile, index);
		Map<String,Map<String, String> > hey = new HashMap<String,Map<String, String>>();
		hey.put(key, value);
		String line = hey + "\n";
		append(filePath, line);
		
		return true;
	}

}
