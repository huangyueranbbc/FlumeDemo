import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import java.nio.charset.Charset;

public class MyApp {
	public static void main(String[] args) {
		MyRpcClientFacade client = new MyRpcClientFacade();
		// Initialize client with the remote Flume agent's host and port
		client.init("192.168.68.129", 44444);

		// Send 10 events to the remote Flume agent. That agent should be
		// configured to listen with an AvroSource.

		// 发送数据 通过rpc avro
		String sampleData = "Hello Flume!";
		for (int i = 0; i < 1000; i++) {
			client.sendDataToFlume(sampleData);
		}

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

	public void cleanUp() {
		// Close the RPC connection
		client.close();
	}

}
