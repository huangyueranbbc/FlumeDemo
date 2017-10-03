#FLUME DEMO  
  
MyApp.java:  
flume-ng agent -c conf -f case_client.conf -n a1 -Dflume.root.logger=INFO,console  
  
FailOverRPC.java	LoadBalanceRPC.java  
flume-ng agent -c conf -f case_client1.conf -n a1 -Dflume.root.logger=INFO,console  
flume-ng agent -c conf -f case_client2.conf -n a1 -Dflume.root.logger=INFO,console  
flume-ng agent -c conf -f case_client3.conf -n a1 -Dflume.root.logger=INFO,console  
  
MySink.java  
flume-ng agent -c conf -f case_custom_sink.conf -n a1 -Dflume.root.logger=INFO,console  
  
MySource.java  
flume-ng agent -c conf -f case_custom_source.conf -n a1 -Dflume.root.logger=INFO,console  