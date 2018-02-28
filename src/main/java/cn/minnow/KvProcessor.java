package cn.minnow;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
	private static final String HDFS_PATH = KvStoreConfig.getHdfsUrl();
	private static final String LOCAL_PATH = "/opt/localdisk";
//	private static final String HDFS_PATH = "hdfs://localhost:9000";
//	private static final String LOCAL_PATH = "d:/cloudindex";
	private static final int ONE_NODE = 66060288;
	
	private boolean flag;
	Map<String, Map<String,String>> mem = null; 
	Gson gson = new Gson();
	
	HdfsOperation hdfs = new HdfsOperation();
	LocalOperation local = new LocalOperation();

	public KvProcessor(){
		System.out.println("new a processor!");
		mem = new ConcurrentHashMap();
		flag = true;	
	}

	@Override
	public boolean batchPut(Map<String, Map<String, String>> records) {
		// TODO Auto-generated method stub
		
		for(Map.Entry<String, Map<String,String>> entry:records.entrySet()) {
			put(entry.getKey(),entry.getValue());
		}
		
//		int kvpodId = RpcServer.getRpcServerId();
//		int num = KvStoreConfig.getServersNum();
//		
//		String filePath = hdfs.whichFile(kvpodId);
//		System.out.println("batch putting... path: " + filePath );
//		
//		Map<String,Map<String,String>> map = new HashMap<String,Map<String,String>>();
//		String lines = "";
//		
//		for(Map.Entry<String, Map<String,String>> entry:map.entrySet()) {
//			Map<String,Map<String,String>> tmp = new HashMap<String,Map<String,String>>();
//			String key = entry.getKey();
//			tmp.put(key, entry.getValue());
//			
//			lines = lines + tmp + "\n";
//			
//			String indexFile = "";
//			switch(Math.abs(key.hashCode()%3)) {
//				case 0:
//					indexFile = "/index0";
//					break;
//				case 1:
//					indexFile = "/index1";
//					break;
//				case 2:
//					indexFile = "/index2";
//					break;	
//			}
//			
//			String index = key + "," + filePath + "\n";
//			local.writeToLocal(LOCAL_PATH+indexFile,index);
//			System.out.println("batch putting... indexing: " + indexFile );
//			
//			//索引备份
//			for(int i = 0; i < num; i++) {
//				if (i == kvpodId)
//					continue;
//				try {
//					RpcClientFactory.inform(i ,index.getBytes());
//				} catch (IOException e) {
//					String indexOnHDFS = hdfs.whichIndexFile(kvpodId,key);
//					hdfs.append(indexOnHDFS, index);
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//			
//		}
//		
//		
//		hdfs.append(filePath, lines);
		
		return true;
	}

	@Override
	public int count(Map<String, String> arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Map<String, String> get(String key) {
		// TODO Auto-generated method stub
		System.out.println("geting... Key: " + key );
		
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
		String filePath = local.readIndex(LOCAL_PATH + indexFile,key);
		int kvpodId = 0;
		while(filePath == null || filePath == "") {
			filePath = hdfs.readHdfsIndex(HDFS_PATH+"/index"+kvpodId+indexFile,key);
			kvpodId++;
		}
		
		System.out.println("geting... indexpath: " + filePath );
		Map<String,String> value = hdfs.readHdfs(HDFS_PATH + filePath,key);
		
		
		System.out.println("get value: " + value );
		return value;
	}

	@Override
	public Map<Map<String, String>, Integer> groupBy(List<String> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] process(byte[] input) {
		// TODO Auto-generated method stub
		
		int kvpodId = RpcServer.getRpcServerId();
		String index = new String(input);
		System.out.println("index:" + index);
		String key = index.split(",")[0];
		System.out.println("key:" + key);
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
		
		System.out.println("id:" + kvpodId + "create index file: "+ LOCAL_PATH+indexFile);
		local.writeToLocal(LOCAL_PATH+indexFile,index);
		
		return "fucked!".getBytes();
	}

	@Override
	public boolean put(String key, Map<String, String> value) {
		// TODO Auto-generated method stub
		System.out.println("Key: " + key + "Value： " + value);
		
		if(flag) {
			System.out.println("new a thread!!!! \n" + flag);
			new Thread(new putToHdfs()).start();
		}
		flag = false;
//		int kvpodId = RpcServer.getRpcServerId();
//		int num = KvStoreConfig.getServersNum();
//		
//		System.out.println("kvpodId:" + kvpodId + " \n num: " + num);
//		
//		String filePath = hdfs.whichFile(kvpodId);
//		String indexFile = "";
//		switch(Math.abs(key.hashCode()%3)) {
//			case 0:
//				indexFile = "/index0";
//				break;
//			case 1:
//				indexFile = "/index1";
//				break;
//			case 2:
//				indexFile = "/index2";
//				break;	
//		}
//		
//		
//		
//		String index = key + "," + filePath + "\n";
//		
//		System.out.println("index:" + index);
//		local.writeToLocal(LOCAL_PATH+indexFile,index);
//		
//		//索引备份
//		for(int i = 0; i < num; i++) {
//			if (i == kvpodId)
//				continue;
//			try {
//				RpcClientFactory.inform(i ,index.getBytes());
//			} catch (IOException e) {
//				System.out.println("inform exception from:"+ kvpodId +"to: "+i);
//				String indexOnHDFS = hdfs.whichIndexFile(kvpodId,key);
//				hdfs.append(indexOnHDFS, index);
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
		synchronized(mem) {
			mem.put(key, value);
		}
		
//		Map<String,Map<String, String> > hey = new HashMap<String,Map<String, String>>();
//		hey.put(key, value);
//		String line = hey + "\n";
		
//		System.out.println("hey:" + line);
//		hdfs.append(filePath, line);
		
		return true;
	}
	
	public void writeToHdfs() {
		System.out.println("start writeToHdfs");
		int kvpodId = RpcServer.getRpcServerId();
		int num = KvStoreConfig.getServersNum();
		String filePath = hdfs.whichFile();
		if(!mem.isEmpty()) {
			for(Map.Entry<String, Map<String,String>> entry:mem.entrySet()) {
				String index = entry.getKey() + "," + filePath + "\n";
				String indexFile = "";
				switch(Math.abs(entry.getKey().hashCode()%3)) {
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
				
				
				System.out.println("index:" + index);
				local.writeToLocal(LOCAL_PATH+indexFile,index);
				
				for(int i = 0; i < num; i++) {
					if (i == kvpodId)
						continue;
					try {
						RpcClientFactory.inform(i ,index.getBytes());
					} catch (IOException e) {
						System.out.println("inform exception from:"+ kvpodId +"to: "+i);
						String indexOnHDFS = hdfs.whichIndexFile(kvpodId,entry.getKey());
						hdfs.append(indexOnHDFS, index);
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			
			synchronized(mem) {
				String memStr = gson.toJson(mem);
				hdfs.append(filePath, memStr);
			}

		}
		
	}

	
	private class putToHdfs implements Runnable {

		@Override
		public void run() {
			System.out.println("start thread");
			long startTime = 0,endTime = 0,totalTime = 0;
			// TODO Auto-generated method stub
			startTime = System.currentTimeMillis();
			while(true) {
				try {
					System.out.println("sleep 0.5 min!");
					
					
					Thread.sleep(6000);
					if(mem.size() > 2000) {
						System.out.println("mem size > 2000");
						writeToHdfs();
						synchronized(mem) {
							mem.clear();
						}
						
						startTime = System.currentTimeMillis();
					}
					endTime   = System.currentTimeMillis(); 
					totalTime = endTime - startTime;
					if(totalTime > 10000 && !mem.isEmpty()) {
						System.out.println("time size > 10000");
						writeToHdfs();
						synchronized(mem) {
							mem.clear();
						}
						startTime = System.currentTimeMillis();
					}
					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
		
	}
	
	
}
