package com.huawei.hwcloud.tarus.kvstore.race;

import com.carrotsearch.hppc.LongIntHashMap;
import com.huawei.hwcloud.tarus.kvstore.common.KVStoreRace;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import com.huawei.hwcloud.tarus.kvstore.util.BufferUtil;
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
import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicInteger;



public class EngineKVStoreRace implements KVStoreRace {

	// 日志
	private static Logger log = LoggerFactory.getLogger(EngineKVStoreRace.class);

	// 不同线程线程名前缀 key文件 01_kv_store.key value文件 01_1.data
	private static final String THREAD_PATH_FORMAT = "00";
	private static final byte base = BufferUtil.stringToBytes("0")[0];


	// keyFile offset: 12B(400w偏移量)   valueFile: 4KB(2^16个偏移量）
	// key长度，Byte为单位
	// 所有offset: 4B keyFile的offset，以及keyFile中offset所指向的value文件中的offset
	private static final int KEY_LEN = 8;
	// offset
	private static final int OFF_LEN = 4;
	private static final int KEY_OFF_LEN = 12;
	// value
	private static final int VALUE_LEN = 4096;	// 4KB
	private static final int SHIF_NUM = 12;		// offset<<12,得到指定地址

	// 单线程写入数据
	private static final int MSG_NUMBER = 4 * 10 ^ 6;

	// 文件数量
	private static final int FILE_COUNT = 64;
	private static final int FILE_SIZE = (1 << (10 * 2 + 8)); //default size: 256MB per file

	// key-off文件
	private static FileChannel keyFileChannel;
	// keyFile文件offset
	private static AtomicInteger keyFileOffset;

	// value文件
	private static FileChannel[] valueFileChannels = new FileChannel[FILE_COUNT];
	// value文件offset
	private static AtomicInteger[] valueFileOffset = new AtomicInteger[FILE_COUNT];

	// hashmap:存储key和offset的映射
	private static final LongIntHashMap keyOffMap = new LongIntHashMap(MSG_NUMBER, 0.99f);

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

		// 在dir文件(父目录）创建该线程对应的value文件
		File dirParent = new File(dir).getParentFile();
		if (!dirParent.exists())
			dirParent.mkdirs();

		// 获取FILE_COUNT个value文件的channel
		RandomAccessFile valueFile;
		for (int i = 0; i < FILE_COUNT; i++) {
			try{
				String valueFileName = fillThreadNo(file_size) + "_" + i + ".data";
				File vFile = new File(dirParent.getPath() + File.separator + valueFileName);
				if (!vFile.exists())
					vFile.createNewFile();

				valueFile = new RandomAccessFile(vFile, "rw");
				valueFileChannels[i] = valueFile.getChannel();
				// valueFileOffset[i]记录的是valueFile[i]下一个要插入值的相对offset  相对偏移量(除去4096)或右移12位
				// valueFile.length()：字节数统计
				valueFileOffset[i] = new AtomicInteger((int)(valueFile.length() >>> SHIF_NUM));

			}catch (IOException e){
				log.warn("open value file={} error", i, e);
			}
		}

		// key-offset 存储文件
		RandomAccessFile randomKeyFile;
		String keyFileName = fillThreadNo(file_size) + "_" + "kv_store.key";
		File keyFile = new File(dirParent.getPath() + File.separator + keyFileName);
		if(!keyFile.exists()){
			try {
				keyFile.createNewFile();
			} catch (IOException e) {
				log.warn("open key offset file error",  e);
			}
		}
		try {
			randomKeyFile = new RandomAccessFile(keyFile, "rw");
			keyFileChannel = randomKeyFile.getChannel();

			keyFileOffset = new AtomicInteger((int)randomKeyFile.length());

			// 使用MMAP读写keyFile
			long keyFileLen = keyFileChannel.size();
			int count = 0;
			MappedByteBuffer keyMmap = keyFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, keyFileLen);

			while (count < keyFileLen) {
				long key = keyMmap.getLong();
				int off = keyMmap.getInt();
				count += (KEY_OFF_LEN);
				keyOffMap.put(key, off);
			}

			unmap(keyMmap);

		} catch (IOException e) {
			log.warn("read key offset file error",  e);
		}

		return true;
	}

	@Override
	public long set(final String key, final byte[] value) throws KVSException {

		byte[] keyBytes = BufferUtil.stringToBytes(key);
		long numKey = bytes2long(keyBytes);
		int fileNo = hash(numKey);

		// valueFile offset
		int offset = keyOffMap.getOrDefault(numKey, -1);

		// value写入buffer
		localBufferValue.get().put(value);
		localBufferValue.get().flip();

		if (offset != -1) {
			// key重复
			try {
				valueFileChannels[fileNo].write(localBufferValue.get(), (long)(offset << SHIF_NUM));
				localBufferValue.get().clear();
			} catch (IOException e) {
				log.warn("write value file={} error", fileNo, e);
			}
		}else{
			try {

				// 获取要插入valueFile的offset，并将当前valueFile的offset加1
				offset = valueFileOffset[fileNo].getAndIncrement();
				// keyOffMap: 存储key和valueOff(插入的起始位置）
				keyOffMap.put(numKey, offset);

				// 先将写入key和offset到keybuffer,写入keyFile文件
				localBufferKey.get().putLong(numKey).putInt(offset);
				localBufferKey.get().flip();
				keyFileChannel.write(localBufferKey.get(), keyFileOffset.getAndAdd(KEY_OFF_LEN));
				localBufferKey.get().clear();

				// 写入value到valueFile文件
				valueFileChannels[fileNo].write(localBufferValue.get(), (long)(offset << SHIF_NUM));
				localBufferValue.get().clear();

			} catch (IOException e) {
				log.warn("write value file={} error", fileNo, e);
			}
		}

		return offset;
	}

	@Override
	public long get(final String key, final Ref<byte[]> val) throws KVSException {

		long numKey = bytes2long(BufferUtil.stringToBytes(key));
		int fileNo = hash(numKey);

		// 获取map中key对应的value文件的offset
		int offset = keyOffMap.getOrDefault(numKey, -1);
		byte[] valByte = localValueBytes.get();

		log.info("get key:{} fileNo:{} offset:{} ", key, fileNo, offset);

		if (offset == -1) {
			// 不存在
			val.setValue(null);
		}else {
			try {
				// 从valueFile中读取
				int len = valueFileChannels[fileNo].read(localBufferValue.get(), (long)(offset << SHIF_NUM));

				localBufferValue.get().flip();
				localBufferValue.get().get(valByte, 0, len);
				localBufferValue.get().clear();

				// 写入到value
				val.setValue(valByte);

			} catch (IOException e) {
				log.warn("read value file={} error", fileNo, e);
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
		try {
			keyFileChannel.close();
		} catch (IOException e) {
			log.warn("close data keyOffset file error!", e);
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

	// key: byte[]-->long
	private static long bytes2long2(byte[] bytes) {
		long result = 0;
		for (int i = 0; i < bytes.length; i++) {
			result <<= 8;
			result |= (bytes[i] & 0xFF);
		}
		return result;
	}

	private static long bytes2long(byte[] bytes) {
		long result = 0;
		for (int i = 0; i < bytes.length; i++) {
			result *= 10;
			result += (bytes[i] - base);
		}
		return result;
	}

	// 64个文件,取余或0x
	private int hash(long key) {
		return (int)(key & 0x3F);
	}

	private static final String fillThreadNo(final int no){
		DecimalFormat df = new DecimalFormat(THREAD_PATH_FORMAT);
		return df.format(Integer.valueOf(no));
	}

	private void unmap(MappedByteBuffer var0) {
		Cleaner var1 = ((DirectBuffer) var0).cleaner();
		if (var1 != null) {
			var1.clean();
		}
	}
}
