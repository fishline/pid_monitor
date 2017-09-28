--set hive.enforce.bucketing=true;
--set hive.enforce.sorting=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000000;
set hive.exec.max.dynamic.partitions=1000000;
set hive.exec.max.created.files=1000000;

set mapreduce.input.fileinputformat.split.minsize=240000000;
set mapreduce.input.fileinputformat.split.maxsize=240000000;
set mapreduce.input.fileinputformat.split.minsize.per.node=240000000;
set mapreduce.input.fileinputformat.split.minsize.per.rack=240000000;
--set hive.exec.parallel=true;
set hive.stats.autogather=true;
set hive.support.concurrency=false;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;

SET hive.exec.compress.output=false;
SET mapreduce.output.fileoutputformat.compress=false;
--SET mapred.output.compression.codec=com.hadoop.compression.lzo.LzoCodec;
--SET mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzoCodec;
set mapred.min.split.size=5368709120;
set mapred.max.split.size=5368709120;
set rcfile.head.compression=true;
set rcfile.head.compressionMethod=org.apache.hadoop.io.compress.GzipCodec;
set rcfile.head.zlibCompressionLevel=bestCompression;
set hive.insert.part.support=true;
set hive.formatstorage.limit.num=2000000000;
set dfs.block.size=268435456;
