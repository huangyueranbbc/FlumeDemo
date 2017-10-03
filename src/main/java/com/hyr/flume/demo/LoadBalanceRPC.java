package com.hyr.flume.demo;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import java.nio.charset.Charset;
import java.util.Properties;

/**
 * @category Flume负载均衡
 * @author huangyueran
 * @time 2017-10-3 12:16:13
 */
public class LoadBalanceRPC {
	public static void main(String[] args) { 
		MyRpcClientFacade2 client = new MyRpcClientFacade2();
		// Initialize client with the remote Flume agent's host and port
		client.init();

		// configured to listen with an AvroSource.
		
		// 发送数据 通过rpc avro 负载均衡
		String sampleData = "LoadBalance!";
		for (int i = 0; i < 10; i++) {
			client.sendDataToFlume(sampleData+"==="+i);
		}

		client.cleanUp();
	}
}

class MyRpcClientFacade2 {
	private RpcClient client; // RPC通信
	private Properties properties;

	/**
	 * @category 初始化,连接flume.使用负载均衡
	 */
	public void init() {
		properties = new Properties();
		properties.put("client.type", "default_loadbalance");

		properties.put("hosts", "h1 h2 h3");

		String host1 = "192.168.68.129:44444";
		String host2 = "192.168.68.129:44445";
		String host3 = "192.168.68.129:44446";

		properties.put("hosts.h1", host1);
		properties.put("hosts.h2", host2);
		properties.put("hosts.h3", host3);
		
		//properties.put("hosts-selector", "random"); // For random host selection 随机选择节点
		properties.put("hosts-selector", "round_robin"); // 轮询 
		properties.put("backoff", "true"); // Disabled by default
		properties.put("maxBackoff", "10000"); // 默认0, 10000=10s  超时时间
		
		// 连接Flume 获取RPC客户端
		client = RpcClientFactory.getInstance(properties);
	}

	/**
	 * @category 向flume source发送数据
	 * @param data
	 */
	public synchronized void sendDataToFlume(String data) {
		// Create a Flume Event object that encapsulates the sample data
		Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));

		// Send the event
		try {
			client.append(event);
		} catch (EventDeliveryException e) {
			// clean up and recreate the client
			client.close();
			client = null;
			client = RpcClientFactory.getInstance(properties);
			// Use the following method to create a thrift client (instead of
			// the above line):
			// this.client = RpcClientFactory.getThriftInstance(hostname, port);
		}
	}

	public void cleanUp() {
		// Close the RPC connection
		client.close();
	}

}
