package com.huawei.hwcloud.tarus.kvstore.race;

import com.carrotsearch.hppc.LongIntHashMap;
import com.huawei.hwcloud.tarus.kvstore.common.KVStoreRace;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import io.netty.util.concurrent.FastThreadLocal;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class EngineKVStoreRace implements KVStoreRace {

	// 日志
	private static Logger log = LoggerFactory.getLogger(EngineKVStoreRace.class);

	// keyFile offset: 12B(400w偏移量)   valueFile: 4KB(2^16个偏移量）
	private static final int KEY_LEN = 8;
	// offset
	private static final int OFF_LEN = 4;
	private static final int KEY_OFF_LEN = 12;
	// value
	private static final int VALUE_LEN = 4096;	// 4KB
	private static final int SHIF_NUM = 12;		// offset<<12

	// 数据量
	private static final int MSG_NUMBER = 4000000;
	private static final int MSG_NUMBER_PER_MAP = 10240;

	// 文件数量:keyFile和valueFile切分1024个分区
	private static final int PARTITION_COUNT = 1024;

    // 多线程读取索引文件，切分为16个索引文件
    private static int THREAD_NUM = 16;

	// key-off文件
	private static FileChannel[] keyFileChannels = new FileChannel[PARTITION_COUNT];
	// value文件
	private static FileChannel[] valueFileChannels = new FileChannel[PARTITION_COUNT];
	// 分区文件offset
	private static AtomicInteger[] partitionOffset = new AtomicInteger[PARTITION_COUNT];

	// hashmap:存储key和offset的映射
	private static final LongIntHashMap keyOffMaps = new LongIntHashMap(MSG_NUMBER, 0.95f);
//	private static final LongIntHashMap[] keyOffMaps = new LongIntHashMap[FILE_COUNT];
//	static {
//	    for (int i = 0; i < THREAD_NUM; i++)
//            keyOffMaps[i] = new LongIntHashMap(MSG_NUMBER_PER_MAP, 0.99f);
//
//    }

	// keyBuffer: 存储keyFile中(key,valueOff)值
	private static FastThreadLocal<ByteBuffer> localBufferKey = new FastThreadLocal<ByteBuffer>() {
		@Override
		protected ByteBuffer initialValue() throws Exception {
			return ByteBuffer.allocateDirect(KEY_LEN + VALUE_LEN);
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


	@Override
	public boolean init(final String dir, final int file_size) throws KVSException {

		// 在dir父目录创建该线程对应文件
		File dirParent = new File(dir).getParentFile();
		// 分线程建目录
//		String dirPath = dirParent.getPath() + File.separator + file_size;
//		File dirFile = new File(dirPath);
		if (!dirParent.exists())
			dirParent.mkdirs();

		// 获取FILE_COUNT个value文件的channel
		RandomAccessFile valueFile;
		for (int i = 0; i < PARTITION_COUNT; i++) {
			try{
				String valueFileName = Utils.fillThreadNo(file_size) + "_" + i + ".data";
				valueFile = new RandomAccessFile(dirParent.getPath() + File.separator + valueFileName, "rw");
				valueFileChannels[i] = valueFile.getChannel();
				// valueFileOffset[i]记录的是valueFile[i]下一个要插入值的相对offset  相对偏移量(除去4096)或右移12位
				partitionOffset[i] = new AtomicInteger((int)(valueFile.length() >>> SHIF_NUM));

			}catch (IOException e){
				log.warn("can't open value file{} in thread {}", i, file_size, e);
			}
		}

		// key-offset 存储文件
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
					keyOffMaps.put(mappedByteBuffer.getLong(), mappedByteBuffer.getInt());
					start += (KEY_OFF_LEN);
				}

				unmap(mappedByteBuffer);
			} catch (IOException e) {
				log.warn("can't open key file{} in thread {}", i, file_size, e);
			}
		}

		/*
		// 多线程读取keyFile缓存map
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_NUM);
        CountDownLatch latch = new CountDownLatch(THREAD_NUM);
        for (int i = 0; i < THREAD_NUM; i++) {
            if (keyFileOffsets[i].get() != 0){
                // keyFile存在记录
                final int index = i;
                final int len = keyFileOffsets[i].get();

                executor.execute(() -> {
                    MappedByteBuffer keyMmap = null;
                    try {
                        keyMmap = keyFileChannels[index].map(FileChannel.MapMode.READ_ONLY, 0, len);
                        int start = 0;
                        long key;
                        int keyFileNo;
                        // 多线程读取keyFile缓存
                        while (start < len) {
                            key = keyMmap.getLong();
                            keyFileNo = Utils.keyFileHash2(key);
                            keyOffMaps[keyFileNo].put(key, keyMmap.getInt());
                            start += KEY_OFF_LEN;
                        }
                    } catch (IOException e) {
                        log.warn("executor fail", e);
                    }
                    unmap(keyMmap);
                    latch.countDown();
                });
            }
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.warn("countdownlatch fail", e);
        }
        executor.shutdown();
		*/

		return true;
	}

	@Override
	public long set(final String key, final byte[] value) throws KVSException {

//		byte[] keyBytes = BufferUtil.stringToBytes(key);
//		long numKey = bytes2long(keyBytes);
		long numKey = Long.parseLong(key);
        int paritionNo = Utils.fileHash(numKey);

        log.info("partition No:{}, key:{}", paritionNo, key);

		// valueFile offset
		int offset = keyOffMaps.getOrDefault(numKey, -1);

		// value写入buffer
		localBufferValue.get().put(value);
		localBufferValue.get().flip();


		if (offset != -1) { // key已存在，更新
			try {
				valueFileChannels[paritionNo].write(localBufferValue.get(), ((long)offset) << SHIF_NUM);
				localBufferValue.get().clear();
			} catch (IOException e) {
				log.warn("write value file={} error", paritionNo, e);
			}
		}else{
			try {

				// 分区文件下一个offset
				offset = partitionOffset[paritionNo].getAndIncrement();

                long valueOff =  ((long)offset) << SHIF_NUM;
                long keyOff =  ((long)offset) * KEY_OFF_LEN;
                // 解决 IllegalArgumentException: Negative position

                log.info("off:{} key:{} value:{}", offset, keyOff, valueOff);

				// keyOffMap: 存储key和valueOff(插入的起始位置）
				keyOffMaps.put(numKey, offset);

				// 先将写入key和offset到keybuffer,写入keyFile文件
				localBufferKey.get().putLong(numKey).putInt(offset);
				localBufferKey.get().flip();
				keyFileChannels[paritionNo].write(localBufferKey.get(), keyOff);
				localBufferKey.get().clear();

				// 写入value到valueFile文件
				valueFileChannels[paritionNo].write(localBufferValue.get(), valueOff);
				localBufferValue.get().clear();

			} catch (IOException e) {
				log.warn("write value file={} error", paritionNo, e);
			}
		}

		return offset;
	}

	@Override
	public long get(final String key, final Ref<byte[]> val) throws KVSException {

//		long numKey = bytes2long(BufferUtil.stringToBytes(key));
        long numKey = Long.parseLong(key);
        int paritionNo = Utils.fileHash(numKey);

        log.info("partition No:{}, key:{}", paritionNo, key);

        // 获取map中key对应的value文件的offset
		int offset = keyOffMaps.getOrDefault(numKey, -1);
		byte[] valByte = localValueBytes.get();

		if (offset == -1) {
			// 不存在
			val.setValue(null);
		}else {
			try {
                long valueOff =  ((long)offset) << SHIF_NUM;
                // 从valueFile中读取
				int len = valueFileChannels[paritionNo].read(localBufferValue.get(), valueOff);

                log.info("off:{} value:{} len:{}, buffer:{}",offset, valueOff, len, localBufferValue.get());

				localBufferValue.get().flip();
				localBufferValue.get().get(valByte, 0, len);
				localBufferValue.get().clear();
				// 写入到value
				val.setValue(valByte);

			} catch (IOException e) {
				log.warn("read value file={} error", paritionNo, e);
			}
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
