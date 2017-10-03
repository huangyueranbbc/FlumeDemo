package com.hyr.flume.demo;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import java.nio.charset.Charset;
import java.util.Properties;

/**
 * @category Flume故障迁移
 * @author huangyueran
 * @time 2017-10-3 12:16:13
 */
public class FailOverRPC {
	public static void main(String[] args) {
		MyRpcClientFacade1 client = new MyRpcClientFacade1();
		// Initialize client with the remote Flume agent's host and port
		client.init();

		// configured to listen with an AvroSource.
		
		// 发送数据 通过rpc avro 有三个Flume节点 如果优先级最高德挂掉,会将它放入池中进行冷却,然后选一台次优先级进行接收
		String sampleData = "Hello Flume Failover!";
		for (int i = 0; i < 1000; i++) {
			client.sendDataToFlume(sampleData);
		}

		client.cleanUp();
	}
}

class MyRpcClientFacade1 {
	private RpcClient client; // RPC通信
	private Properties properties;

	/**
	 * @category 初始化,连接flume.使用故障迁移
	 */
	public void init() {
		properties = new Properties();
		properties.put("client.type", "default_failover");

		properties.put("hosts", "h1 h2 h3");

		String host1 = "192.168.68.129:44444";
		String host2 = "192.168.68.129:44445";
		String host3 = "192.168.68.129:44446";

		properties.put("hosts.h1", host1);
		properties.put("hosts.h2", host2);
		properties.put("hosts.h3", host3);
		// 连接Flume 获取RPC客户端
		client = RpcClientFactory.getInstance(properties);
	}

	/**
	 * @category 向flume source发送数据
	 * @param data
	 */
	public void sendDataToFlume(String data) {
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
