package cn.minnow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import cn.helium.kvstore.common.KvStoreConfig;

public class LocalOperation {
	
	private Configuration conf = new Configuration();
	private static final String HDFS_PATH = KvStoreConfig.getHdfsUrl();
	private static final int ONE_NODE = 66060288;
	
	LocalOperation(){
		System.out.println("new a local operation!");
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

}
