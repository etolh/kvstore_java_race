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
	// 所有offset: 4B keyFile的offset，以及keyFile中offset所指向的value文件中的offset
	private static final int KEY_LEN = 8;
	// offset
	private static final int OFF_LEN = 4;
	private static final int KEY_OFF_LEN = 12;
	// value
	private static final int VALUE_LEN = 4096;	// 4KB
	private static final int SHIF_NUM = 12;		// offset<<12,得到指定地址

	// 单线程写入数据
//	private static final int MSG_NUMBER = 4 * 10 ^ 6;
	private static final int MSG_NUMBER_PER_MAP = 320000;

	// 文件数量
	private static final int FILE_COUNT = 64;
//	private static final int FILE_SIZE = (1 << (10 * 2 + 8)); //default size: 256MB per file

    // 多线程读取索引文件，切分为16个索引文件
    private static int THREAD_NUM = 16;

	// key-off文件
	private static FileChannel[] keyFileChannels = new FileChannel[THREAD_NUM];
	// keyFile文件offset
	private static AtomicInteger[] keyFileOffsets = new AtomicInteger[THREAD_NUM];
	// value文件
	private static FileChannel[] valueFileChannels = new FileChannel[FILE_COUNT];
	// value文件offset
	private static AtomicInteger[] valueFileOffset = new AtomicInteger[FILE_COUNT];

	// hashmap:存储key和offset的映射
	private static final LongIntHashMap[] keyOffMaps = new LongIntHashMap[THREAD_NUM];
	static {
	    for (int i = 0; i < THREAD_NUM; i++)
            keyOffMaps[i] = new LongIntHashMap(MSG_NUMBER_PER_MAP, 0.95f);

    }

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
		String dirPath = dirParent.getPath() + File.separator + file_size;
		File dirFile = new File(dirPath);
		if (!dirFile.exists())
			dirFile.mkdirs();

		// 获取FILE_COUNT个value文件的channel
		RandomAccessFile valueFile;
		for (int i = 0; i < FILE_COUNT; i++) {
			try{
				valueFile = new RandomAccessFile(dirPath + File.separator + i + ".data", "rw");
				valueFileChannels[i] = valueFile.getChannel();
				// valueFileOffset[i]记录的是valueFile[i]下一个要插入值的相对offset  相对偏移量(除去4096)或右移12位
				valueFileOffset[i] = new AtomicInteger((int)(valueFile.length() >>> SHIF_NUM));

			}catch (IOException e){
				log.warn("can't open value file{} in thread {}", i, file_size, e);
			}
		}

		// key-offset 存储文件
		RandomAccessFile keyFile;
		for (int i = 0; i < THREAD_NUM; i++) {
			try {
				keyFile = new RandomAccessFile(dirPath + File.separator + i  + ".key", "rw");
				keyFileChannels[i] = keyFile.getChannel();
				keyFileOffsets[i] = new AtomicInteger((int)keyFile.length());
			} catch (IOException e) {
				log.warn("can't open key file{} in thread {}", i, file_size, e);
			}
		}

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

		return true;
	}

	@Override
	public long set(final String key, final byte[] value) throws KVSException {

//		byte[] keyBytes = BufferUtil.stringToBytes(key);
//		long numKey = bytes2long(keyBytes);
		long numKey = Long.parseLong(key);
        int keyFileNo = Utils.keyFileHash2(numKey);
        int valueFileNo = Utils.valueFileHash2(numKey);

		// valueFile offset
		int offset = keyOffMaps[keyFileNo].getOrDefault(numKey, -1);

		// value写入buffer
		localBufferValue.get().put(value);
		localBufferValue.get().flip();

		if (offset != -1) { // key已存在，更新
			try {
				valueFileChannels[valueFileNo].write(localBufferValue.get(), (long)(offset << SHIF_NUM));
				localBufferValue.get().clear();
			} catch (IOException e) {
				log.warn("write value file={} error", valueFileNo, e);
			}
		}else{
			try {

				// 获取要插入valueFile的offset，并将当前valueFile的offset加1
				offset = valueFileOffset[valueFileNo].getAndIncrement();
				// keyOffMap: 存储key和valueOff(插入的起始位置）
				keyOffMaps[keyFileNo].put(numKey, offset);

				// 先将写入key和offset到keybuffer,写入keyFile文件
				localBufferKey.get().putLong(numKey).putInt(offset);
				localBufferKey.get().flip();
				keyFileChannels[keyFileNo].write(localBufferKey.get(), keyFileOffsets[keyFileNo].getAndAdd(KEY_OFF_LEN));
				localBufferKey.get().clear();

				// 写入value到valueFile文件
				valueFileChannels[valueFileNo].write(localBufferValue.get(), (long)(offset << SHIF_NUM));
				localBufferValue.get().clear();

			} catch (IOException e) {
				log.warn("write value file={} error", valueFileNo, e);
			}
		}

		return offset;
	}

	@Override
	public long get(final String key, final Ref<byte[]> val) throws KVSException {

//		long numKey = bytes2long(BufferUtil.stringToBytes(key));
        long numKey = Long.parseLong(key);
        int keyFileNo = Utils.keyFileHash2(numKey);
        int valueFileNo = Utils.valueFileHash2(numKey);

		// 获取map中key对应的value文件的offset
		int offset = keyOffMaps[keyFileNo].getOrDefault(numKey, -1);
		byte[] valByte = localValueBytes.get();

		if (offset == -1) {
			// 不存在
			val.setValue(null);
		}else {
			try {
				// 从valueFile中读取
				int len = valueFileChannels[valueFileNo].read(localBufferValue.get(), (long)(offset << SHIF_NUM));

				localBufferValue.get().flip();
				localBufferValue.get().get(valByte, 0, len);
				localBufferValue.get().clear();
				// 写入到value
				val.setValue(valByte);

			} catch (IOException e) {
				log.warn("read value file={} error", valueFileNo, e);
			}
		}
		return 0;
	}

	@Override
	public void close() {
		for (int i = 0; i < FILE_COUNT; i++) {
			try {
				valueFileChannels[i].close();
			} catch (IOException e) {
				log.warn("close data file={} error!", i, e);
			}
		}

        for (int i = 0; i < THREAD_NUM; i++) {
            try {
                keyFileChannels[i].close();
            } catch (IOException e) {
                log.warn("close key file={} error!", i, e);
            }
        }

	}

	@Override
	public void flush() {
		for (int i = 0; i < FILE_COUNT; i++) {
			if (valueFileChannels[i] != null && valueFileChannels[i].isOpen()){
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
