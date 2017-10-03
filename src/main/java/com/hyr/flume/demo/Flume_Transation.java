package com.hyr.flume.demo;

import java.nio.charset.Charset;

import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.EventBuilder;

/**
 * @category Transation
 * @author huangyueran
 * @time 2017-10-3 12:16:13
 */
public class Flume_Transation {

	public static void main(String[] args) {
		Channel ch = new MemoryChannel();
		Transaction txn = ch.getTransaction();
		// 开启事务
		txn.begin();
		try {
			// TODO This try clause includes whatever Channel operations you
			// want to
			// do

			Event eventToStage = EventBuilder.withBody("Hello Flume!", Charset.forName("UTF-8"));
			ch.put(eventToStage);
			// Event takenEvent = ch.take();
			// ...

			// 提交事务
			txn.commit();
		} catch (Throwable t) {
			txn.rollback();

			// Log exception, handle individual exceptions as needed

			// re-throw all Errors
			if (t instanceof Error) {
				throw (Error) t;
			}
		} finally {
			txn.close();
		}
	}
}
