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
	
	KvProcessor(){
		System.out.println("new a processor!");
	}

	private Configuration conf = new Configuration();
	private static final String HDFS_PATH = "hdfs://localhost:9000";
	private static final int ONE_NODE = 66060288;
	
	HdfsOperation hdfs = new HdfsOperation();
	LocalOperation local = new LocalOperation();

	public static void main(String args[]) {
		

		System.out.println("success!");
	}
	


	@Override
	public boolean batchPut(Map<String, Map<String, String>> records) {
		// TODO Auto-generated method stub
		
		int kvpodId = RpcServer.getRpcServerId();
		int num = KvStoreConfig.getServersNum();
		
		String filePath = hdfs.whichFile(kvpodId);
		
		Map<String,Map<String,String>> map = new HashMap<String,Map<String,String>>();
		String lines = "";
		
		for(Map.Entry<String, Map<String,String>> entry:map.entrySet()) {
			Map<String,Map<String,String>> tmp = new HashMap<String,Map<String,String>>();
			String key = entry.getKey();
			tmp.put(key, entry.getValue());
			
			lines = lines + tmp + "\n";
			
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
			local.writeToLocal("D:/cloudindex"+indexFile,index);
			
			//索引备份
			for(int i = 0; i < num; i++) {
				if (i == kvpodId)
					continue;
				try {
					RpcClientFactory.inform(i ,index.getBytes());
				} catch (IOException e) {
					String indexOnHDFS = hdfs.whichIndexFile(kvpodId,key);
					hdfs.append(indexOnHDFS, index);
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
		
		
		hdfs.append(filePath, lines);
		
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
		String filePath = local.readIndex("D:/cloudindex" + indexFile,key);
		int kvpodId = 0;
		while(filePath == null || filePath == "") {
			filePath = hdfs.readHdfsIndex(HDFS_PATH+"/index"+kvpodId+indexFile,key);
			kvpodId++;
		}
		
		Map<String,String> value = hdfs.readHdfs(HDFS_PATH + filePath,key);
		
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
		
		String index = new String(input);
		String key = index.split(",")[0];
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
		
		local.writeToLocal("D:/cloudindex"+indexFile,index);
		
		return null;
	}

	@Override
	public boolean put(String key, Map<String, String> value) {
		// TODO Auto-generated method stub
		int kvpodId = RpcServer.getRpcServerId();
		int num = KvStoreConfig.getServersNum();
		
		String filePath = hdfs.whichFile(kvpodId);
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
		local.writeToLocal("D:/cloudindex"+indexFile,index);
		
		//索引备份
		for(int i = 0; i < num; i++) {
			if (i == kvpodId)
				continue;
			try {
				RpcClientFactory.inform(i ,index.getBytes());
			} catch (IOException e) {
				String indexOnHDFS = hdfs.whichIndexFile(kvpodId,key);
				hdfs.append(indexOnHDFS, index);
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		Map<String,Map<String, String> > hey = new HashMap<String,Map<String, String>>();
		hey.put(key, value);
		String line = hey + "\n";
		hdfs.append(filePath, line);
		
		return true;
	}

}
