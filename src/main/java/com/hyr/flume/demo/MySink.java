package com.hyr.flume.demo;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

/**
 * @category 自定义Sink
 * @author huangyueran
 * @info 除了配置文件外,还需要将该文件的jar包放到flume服务端的lib目录下
 * @time 2017-10-3 12:16:13
 */
public class MySink extends AbstractSink implements Configurable {
	  private String myProp;

	  @Override
	  public void configure(Context context) {
	    String myProp = context.getString("myProp", "defaultValue");

	    // Process the myProp value (e.g. validation)

	    // Store myProp for later retrieval by process() method
	    this.myProp = myProp;
	  }

	  @Override
	  public void start() {
	    // Initialize the connection to the external repository (e.g. HDFS) that
	    // this Sink will forward Events to ..
	  }

	  @Override
	  public void stop () {
	    // Disconnect from the external respository and do any
	    // additional cleanup (e.g. releasing resources or nulling-out
	    // field values) ..
	  }

	  @Override
	  public Status process() throws EventDeliveryException {
	    Status status = null;

	    // Start transaction
	    Channel ch = getChannel();
	    Transaction txn = ch.getTransaction();
	    txn.begin();
	    try {
	      // This try clause includes whatever Channel operations you want to do

	      Event event = ch.take();

	      // Send the Event to the external repository.
	      // storeSomeData(e);
	      System.out.println(new String(event.getBody()));
	      txn.commit();
	      status = Status.READY;
	    } catch (Throwable t) {
	      txn.rollback();

	      // Log exception, handle individual exceptions as needed

	      status = Status.BACKOFF;

	      // re-throw all Errors
	      if (t instanceof Error) {
	        throw (Error)t;
	      }
	    } finally {
	      txn.close();
	    }
	    return status;
	  }
	}
