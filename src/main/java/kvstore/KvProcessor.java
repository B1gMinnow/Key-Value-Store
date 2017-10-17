package kvstore;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;

import cn.helium.kvstore.processor.Processor;
import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.rpc.*; 
public class KvProcessor implements Processor{
	KvProcessor(){}
	private Configuration conf = new Configuration();
		
	@Override
	public boolean batchPut(Map<String, Map<String, String>> arg0) {
		// TODO Auto-generated method stub
		
		return false;
	}

	@Override
	public Map<String, String> get(String key) {
		// TODO Auto-generated method stub
		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(KvStoreConfig.getHdfsUrl()+key);
			if(fs.exists(path)){
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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

		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(KvStoreConfig.getHdfsUrl()+key);
			FSDataOutputStream output = fs.create(path);
			byte[] buff = (new Gson().toJson(value)).getBytes();
			output.write(buff,0,buff.length);
			output.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("FileSystem IOException:" + e);
		}catch(Exception e) {
			e.printStackTrace();
		}
		return true;
	}
	
	public static void main(String args[]) {
		KvStoreConfig config = new KvStoreConfig();
		System.out.println("url:" + config.getServersNum());
	}

}
