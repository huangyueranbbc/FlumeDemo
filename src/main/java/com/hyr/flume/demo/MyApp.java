package com.hyr.flume.demo;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * @category 使用RPC和FLume通信
 * @author huangyueran
 * @time 2017-10-3 12:16:13
 */
public class MyApp {
	public static void main(String[] args) {
		MyRpcClientFacade client = new MyRpcClientFacade();
		// Initialize client with the remote Flume agent's host and port
		client.init("192.168.68.129", 44444);

		// Send 10 events to the remote Flume agent. That agent should be
		// configured to listen with an AvroSource.

		// 单条发送数据append 通过rpc avro 
		String sampleData = "Hello Flume!";
		//		for (int i = 0; i < 1000; i++) {
		//			client.sendDataToFlume(sampleData);
		//		}

		// 批量发送数据 appendBatch
		List<String> sampleDatas=new ArrayList<String>();
		for (int i = 0; i < 100; i++) {
			sampleDatas.add(sampleData+"="+i);
		}
		client.sendDataListToFlumeBatch(sampleDatas);
		
		client.cleanUp();
	}
}

class MyRpcClientFacade {
	private RpcClient client; // RPC通信
	private String hostname; // Flume主机IP
	private int port; // Flume的Source端口

	/**
	 * @category 初始化,连接flume
	 * @param hostname
	 *            ip地址
	 * @param port
	 *            端口
	 */
	public void init(String hostname, int port) {
		// Setup the RPC connection
		this.hostname = hostname;
		this.port = port;
		this.client = RpcClientFactory.getDefaultInstance(hostname, port);
		// Use the following method to create a thrift client (instead of the
		// above line):
		// this.client = RpcClientFactory.getThriftInstance(hostname, port);
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
			client = RpcClientFactory.getDefaultInstance(hostname, port);
			// Use the following method to create a thrift client (instead of
			// the above line):
			// this.client = RpcClientFactory.getThriftInstance(hostname, port);
		}
	}
	
	/**
	 * @category 向flume source批量发送数据,appendBatch
	 * @param data
	 */
	public void sendDataListToFlumeBatch(List<String> datas) {
		// Create a Flume Event object that encapsulates the sample data
		List<Event> events=new ArrayList<Event>();
		
		for(String data :datas){
			Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
			events.add(event);
		}

		// Send the event
		try {
			client.appendBatch(events);
		} catch (EventDeliveryException e) {
			// clean up and recreate the client
			client.close();
			client = null;
			client = RpcClientFactory.getDefaultInstance(hostname, port);
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
