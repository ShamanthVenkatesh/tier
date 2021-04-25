#!bin/sh
export=HADOOP_CONF_DIR=/etc/hadoop/conf:/etc/hive/conf:/etc/hbase/conf
spark2-submit --master yarn --class com.coding.challenge.tier.LoadTrackEventsData --name Load-TrackEvents-Json --driver-memory 25G --conf spark.submit.deployMode=cluster --conf spark.sql.shuffle.partitions=400 --conf spark.driver.maxResultSize=15g --conf spark.core.connection.ack.wait.timeout=600 --conf spark.network.timeout=2000 --conf spark.akka.frameSize=500 --conf spark.io.compression.codec=org.apache.spark.io.LZ4CompressionCodec --conf spark.speculation=false --conf spark.hadoop.mapred.output.committer.class=org.apache.hadoop.mapred.DirectFileOutputCommitter --conf spark.hadoop.mapreduce.use.directfileoutputcommitter=true --conf spark.memory.storageFraction=0.35 --conf spark.shuffle.memoryFraction=0.4 --conf spark.executor.memoryOverhead=12G --conf spark.driver.memoryOverhead=12G --conf spark.kryoserializer.buffer.max=1024mb --executor-memory 5G --executor-cores 5 /Users/shvenkat/workspace/tier/tier/target/tier-0.0.1-SNAPSHOT-jar-with-dependencies.jar $*

hive -e "MSCK REPAIR TABLE shamanth.track_events"