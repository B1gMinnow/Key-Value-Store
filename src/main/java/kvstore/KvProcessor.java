package kvstore;
import java.util.*;

import cn.helium.kvstore.processor.Processor;
import cn.helium.kvstore.common.KvStoreConfig;
public class KvProcessor implements Processor{

	@Override
	public boolean batchPut(Map<String, Map<String, String>> arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<String, String> get(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] process(byte[] arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean put(String arg0, Map<String, String> arg1) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public static void main(String args[]) {
		KvStoreConfig config = new KvStoreConfig();
		System.out.println("url:" + config.getServersNum());
	}

}
