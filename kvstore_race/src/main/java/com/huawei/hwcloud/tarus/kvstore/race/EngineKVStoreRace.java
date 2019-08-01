package com.huawei.hwcloud.tarus.kvstore.race;

import com.carrotsearch.hppc.LongLongHashMap;
import com.huawei.hwcloud.tarus.kvstore.common.KVStoreRace;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import com.huawei.hwcloud.tarus.kvstore.race.common.Utils;
import io.netty.util.concurrent.FastThreadLocal;
import moe.cnkirito.kdio.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

public class EngineKVStoreRace implements KVStoreRace {
	private static Logger log = LoggerFactory.getLogger(EngineKVStoreRace.class);
	// key:8B fileNo:2B off:2B
	private static final int KEY_OFF_LEN = 16;
	// value
	private static final int VALUE_LEN = 4096;	// 4KB
	private static final int SHIF_NUM = 12;		// offset<<12
	// 数据量
	private static final int MSG_NUMBER = 4096000;
	// 文件数量:keyFile和valueFile切分1024个分区
	private static final int PARTITION_COUNT = 1024;
	// 多线程读取索引文件，切分为16个索引文件
//    private static int THREAD_NUM = 16;
	// 一个分区最多存储:4000*1024>4000000
	private static final int KV_NUMBER_PER_PAR = 4000;

	// 记录当前区分号，当读取超过4000次时，分区+1
	private AtomicInteger partitionNo = new AtomicInteger(0);
	// 分区文件offset
	private AtomicInteger[] partitionOffset = new AtomicInteger[PARTITION_COUNT];

	// key-off文件
	private FileChannel[] keyFileChannels = new FileChannel[PARTITION_COUNT];
	// value文件
	private FileChannel[] valueFileChannels = new FileChannel[PARTITION_COUNT];

	// hashmap:存储key和offset的映射
	// key:(parNo, offset)
	private LongLongHashMap keyOffMaps = new LongLongHashMap(MSG_NUMBER, 0.99f);

	// keyBuffer: 存储keyFile中(key,valueOff)值
	private static FastThreadLocal<ByteBuffer> localBufferKey = new FastThreadLocal<ByteBuffer>() {
		@Override
		protected ByteBuffer initialValue() throws Exception {
			return ByteBuffer.allocateDirect(KEY_OFF_LEN);
		}
	};

	// valueBuffer: 存储value
	private static FastThreadLocal<ByteBuffer> localBufferValue = new FastThreadLocal<ByteBuffer>() {
		@Override
		protected ByteBuffer initialValue() throws Exception {
			return ByteBuffer.allocateDirect(VALUE_LEN);
		}
	};

	// 线程私有的buffer，用于byte数组转long
	private static FastThreadLocal<byte[]> localValueBytes = new FastThreadLocal<byte[]>() {
		@Override
		protected byte[] initialValue() throws Exception {
			return new byte[VALUE_LEN];
		}
	};

	private DirectIOLib directIOLib;
	private int threadID;
	private String filePath;
//	private DirectChannel[] keyFileChannelsDIO = new DirectChannelImpl[PARTITION_COUNT];
//	private DirectChannel[] valueFileChannelsDIO = new DirectChannelImpl[PARTITION_COUNT];

	@Override
	public boolean init(final String dir, final int file_size) throws KVSException {

		this.directIOLib = DirectIOLib.getLibForPath("/");
		this.threadID = file_size;

		// 在dir父目录创建该线程对应文件
		File dirParent = new File(dir).getParentFile();
		if (!dirParent.exists())
			dirParent.mkdirs();

		this.filePath = dirParent.getPath();

		if (directIOLib.binit) {	// DIO模式
			DirectRandomAccessFile valueFile;
			for (int i = 0; i < PARTITION_COUNT; i++) {
				String valueFileName = Utils.fillThreadNo(file_size) + "_" + i + ".data";
				try {
					valueFile = new DirectRandomAccessFile(new File(dirParent.getPath() + File.separator + valueFileName), "rw");
					partitionOffset[i] = new AtomicInteger((int)valueFile.length());
					if (partitionOffset[i].get() >= KV_NUMBER_PER_PAR) {
						partitionNo.getAndIncrement();
					}
				} catch (IOException e) {
					log.warn("init: can't open value file{} in thread {}", i, file_size, e);
				}
			}

			DirectRandomAccessFile keyFile;
			for (int i = 0; i < PARTITION_COUNT; i++) {
				String keyFileName = Utils.fillThreadNo(file_size) + "_" + i + ".key";
				try {
					keyFile = new DirectRandomAccessFile(new File(dirParent.getPath() + File.separator + keyFileName), "rw");
					// 读取key构建map
					long keyLen = keyFile.length();
					ByteBuffer keyBuffer = DirectIOUtils.allocateForDirectIO(directIOLib, (int)keyLen);

					int start =0;
					while (start < keyLen) {
						keyOffMaps.put(keyBuffer.getLong(), keyBuffer.getLong());
						start += KEY_OFF_LEN;
					}
				} catch (IOException e) {
					log.warn("init: can't open key file{} in thread {}", i, file_size, e);
				}
			}
		}
		else {
			// 获取FILE_COUNT个value文件的channel
			RandomAccessFile valueFile;
			for (int i = 0; i < PARTITION_COUNT; i++) {
				try {
					String valueFileName = Utils.fillThreadNo(file_size) + "_" + i + ".data";
					valueFile = new RandomAccessFile(dirParent.getPath() + File.separator + valueFileName, "rw");
					valueFileChannels[i] = valueFile.getChannel();

					partitionOffset[i] = new AtomicInteger((int) (valueFile.length() >>> SHIF_NUM));
					if (partitionOffset[i].get() >= KV_NUMBER_PER_PAR) {    // 分区已满
						partitionNo.getAndIncrement();    // 选择下一分区
					}
				} catch (IOException e) {
					log.warn("init: can't open value file{} in thread {}", i, file_size, e);
				}
			}

			// key文件
			RandomAccessFile keyFile;
			for (int i = 0; i < PARTITION_COUNT; i++) {
				try {
					String keyFileName = Utils.fillThreadNo(file_size) + "_" + i + ".key";
					keyFile = new RandomAccessFile(dirParent.getPath() + File.separator + keyFileName, "rw");
					keyFileChannels[i] = keyFile.getChannel();

					long keyLen = keyFile.length();
					int start = 0;

					MappedByteBuffer mappedByteBuffer = keyFileChannels[i].map(FileChannel.MapMode.READ_ONLY, 0, keyLen);
					while (start < keyLen) {
						// 存储key和的offset映射，：offset:key和value在各自文件插入的偏移量(个数)
						keyOffMaps.put(mappedByteBuffer.getLong(), mappedByteBuffer.getLong());
						start += (KEY_OFF_LEN);
					}

					unmap(mappedByteBuffer);
				} catch (IOException e) {
					log.warn("init: can't open key file{} in thread {}", i, file_size, e);
				}
			}
		}
//		log.info("init");

		return true;
	}

	@Override
	public long set(final String key, final byte[] value) throws KVSException {

		long numKey = Long.parseLong(key);
		// 从map读取判断是否已经重复存储
//		long pos = keyOffMaps.getOrDefault(numKey, -1);
		int parNo = partitionNo.get();
		int offset = partitionOffset[parNo].getAndIncrement();
		if (offset >= KV_NUMBER_PER_PAR) {  // off >= 4000 当前分区已满，放到下一个分区
			partitionNo.incrementAndGet(); // 分区+1
			parNo = partitionNo.get();
			offset = partitionOffset[parNo].getAndIncrement();
		}
		long valueOff = ((long) offset) << SHIF_NUM;
		long keyOff = ((long) offset) * KEY_OFF_LEN;
		// partitionNo和offset组成long，各自32位
		long partitionOff = Utils.combine(parNo, offset);
		localBufferKey.get().putLong(numKey).putLong(partitionOff);
		localBufferKey.get().flip();
		// keyOffMap: key -> (par, offset)
		keyOffMaps.put(numKey, partitionOff);

		if (directIOLib.binit){
			String valueFileName = Utils.fillThreadNo(threadID) + "_" + parNo + ".data";
			ByteBuffer valBuffer = DirectIOUtils.allocateForDirectIO(directIOLib, VALUE_LEN);
			valBuffer.put(value);
			valBuffer.flip();
			try {
				DirectRandomAccessFile directRandomAccessFile = new DirectRandomAccessFile(new File(filePath + File.separator + valueFileName), "rw");
				directRandomAccessFile.write(valBuffer, valueOff);
			} catch (IOException e) {
				log.warn("set: open value file error Partition={} off={}", parNo, offset, e);
			}

		}else {
			// value写入buffer
			localBufferValue.get().put(value);
			localBufferValue.get().flip();

			try {
				// 解决 IllegalArgumentException: Negative position
//				log.info("partition No:{} key:{} off:{}  keyOff:{} valueOff:{} buffer:{}", parNo, key, offset, keyOff, valueOff, localBufferKey.get());
				// 解决读取分区错误问题51
//				log.info("set: partition No:{} key:{} off:{}   partitionOff{}", parNo, key, offset, partitionOff);
				keyFileChannels[parNo].write(localBufferKey.get(), keyOff);
				localBufferKey.get().clear();

				// 写入value到valueFile文件
				int len = valueFileChannels[parNo].write(localBufferValue.get(), valueOff);
				localBufferValue.get().clear();

//				log.info("set: partition No:{}  off:{} valueOff:{} key:{} writeLen:{}", parNo,  offset, key, valueOff, len);
				// Error: 查看channel size,判断是否写入
//			log.info("set: key:{} partition No:{} offset:{} valueOff:{}  channelSize:{} channelSize2:{} len:{}",key, parNo, offset, valueOff, valueFileChannels[parNo].size(), valueFileChannels[parNo].size()>>>SHIF_NUM, len);

			} catch (IOException e) {
				log.warn("set: value Partition={} off={} error", parNo, offset, e);
			}
		}
		return 0;
	}

	@Override
	public long get(final String key, final Ref<byte[]> val) throws KVSException {

        long numKey = Long.parseLong(key);
		// 获取映射
        long partitionOff = keyOffMaps.getOrDefault(numKey, -1);

		if (partitionOff == -1) {
			val.setValue(null);

		}else {

			int[] coms = Utils.divide(partitionOff);
			int offset = coms[0];
			int parNo = coms[1];
			long valueOff = ((long) offset) << SHIF_NUM;
			byte[] valByte = localValueBytes.get();

			if (directIOLib.binit)
			{
				String valueFileName = Utils.fillThreadNo(threadID) + "_" + parNo + ".data";
				ByteBuffer valBuffer = DirectIOUtils.allocateForDirectIO(directIOLib, VALUE_LEN);
				try {
					DirectRandomAccessFile directRandomAccessFile = new DirectRandomAccessFile(new File(filePath + File.separator + valueFileName), "rw");
					int len = directRandomAccessFile.read(valBuffer, valueOff);
					valBuffer.flip();
					valBuffer.get(valByte, 0, len);
					valBuffer.clear();
				} catch (IOException e) {
					log.warn("get: openb value file={} off={} error", parNo, offset, e);
				}
			}
			else
			{
				try {
					int len = valueFileChannels[parNo].read(localBufferValue.get(), valueOff);
					// Error: 查看channel size,判断是否写入
//				log.info("get: key:{} partition No:{} offset:{} valueOff:{} channelSize:{}  channelSize2:{}",key, parNo, offset, valueOff, valueFileChannels[parNo].size(), valueFileChannels[parNo].size()>>>SHIF_NUM);
					// len:解决IndexOutOfBoundsException: 查看是否从valueFile中读取到数据
//                log.info("partition No:{}, key:{} off:{} valueOff:{} partitionOff:{} len:{}, buffer:{}",parNo, key, offset,valueOff, partitionOff, len, localBufferValue.get());

					// 写入到value
					localBufferValue.get().flip();
					localBufferValue.get().get(valByte, 0, len);
					localBufferValue.get().clear();

				} catch (IOException e) {
					log.warn("get: value file={} off={} error", parNo, offset, e);
				}
			}
			val.setValue(valByte);
		}
		return 0;
	}

	@Override
	public void close() {
		for (int i = 0; i < PARTITION_COUNT; i++) {
			try {
				keyFileChannels[i].close();
				valueFileChannels[i].close();
			} catch (IOException e) {
				log.warn("close data file={} error!", i, e);
			}
		}
	}

	@Override
	public void flush() {
		for (int i = 0; i < PARTITION_COUNT; i++) {
			if (valueFileChannels[i] != null && valueFileChannels[i].isOpen()){
				flush(keyFileChannels[i]);
				flush(valueFileChannels[i]);
			}
		}
	}

	// flush channel data to disk
	private void flush(FileChannel channel) {
		if (channel != null && channel.isOpen()){
			try {
				channel.force(false);
			} catch (IOException e) {
				log.warn("flush data file error!");
			}
		}
	}


	private void unmap(MappedByteBuffer var0) {
		Cleaner var1 = ((DirectBuffer) var0).cleaner();
		if (var1 != null) {
			var1.clean();
		}
	}
}
