package com.hyr.flume.demo;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;

/**
 * @category 自定义Source
 * @author huangyueran
 * @info 除了配置文件外,还需要将该文件的jar包放到flume服务端的lib目录下
 * @time 2017-10-3 12:16:13
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {
	  private String myProp;

	  @Override
	  public void configure(Context context) {
	    String myProp = context.getString("myProp", "defaultValue");

	    // Process the myProp value (e.g. validation, convert to another type, ...)

	    // Store myProp for later retrieval by process() method
	    this.myProp = myProp;
	  }

	  @Override
	  public void start() {
	    // Initialize the connection to the external client
	  }

	  @Override
	  public void stop () {
	    // Disconnect from external client and do any additional cleanup
	    // (e.g. releasing resources or nulling-out field values) ..
	  }

	@Override
	public Status process() throws EventDeliveryException {
		// TODO Auto-generated method stub
		List<Channel> list = getChannelProcessor().getSelector().getAllChannels();
		
		Status status = null;

	    // Start transaction
		Channel ch = list.get(0);
	    Transaction txn = ch.getTransaction();
	    txn.begin();
	    try {
	      // This try clause includes whatever Channel operations you want to do

	      // Receive new data
	      Event e = null;//getSomeData();EventBuilder.withBody(data, Charset.forName("UTF-8"), headerMap);EventBuilder.withBody(data, Charset.forName("UTF-8"));

	      // Store the Event into this Source's associated Channel(s)
	      getChannelProcessor().processEvent(e);

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

	@Override
	public long getBackOffSleepIncrement() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getMaxBackOffSleepInterval() {
		// TODO Auto-generated method stub
		return 0;
	}

}
