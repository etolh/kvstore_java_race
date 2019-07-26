package com.huawei.hwcloud.tarus.kvstore.race;

import com.carrotsearch.hppc.LongIntHashMap;
import com.huawei.hwcloud.tarus.kvstore.common.KVStoreRace;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSErrorCode;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import com.huawei.hwcloud.tarus.kvstore.util.BufferUtil;
import io.netty.util.concurrent.FastThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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

			// 读取keyFileChannel中的key和offset
			ByteBuffer keyBuffer = ByteBuffer.allocate(KEY_LEN);	// 8B
			ByteBuffer offsetBuffer = ByteBuffer.allocate(OFF_LEN);	//4B

			// keyFileOffset记录keyFile下一个要插入的位置的绝对offset：相对于1B
			keyFileOffset = new AtomicInteger((int)randomKeyFile.length());
			long index = 0, size = (long) keyFileOffset.get();

			while (index < size) {
				keyBuffer.clear();
				keyFileChannel.read(keyBuffer, index);
				index += KEY_LEN;

				offsetBuffer.clear();
				keyFileChannel.read(offsetBuffer, index);
				index += OFF_LEN;

				keyBuffer.flip();
				offsetBuffer.flip();
				keyOffMap.put(keyBuffer.getLong(), offsetBuffer.getInt());
			}

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
//		log.info("set: key:{} valueLength:{} value:{} numKey:{} fileNo:{} ", key, value.length, new String(value), numKey, fileNo);

		// valueFile offset
		int offset = keyOffMap.getOrDefault(numKey, -1);

		// value写入buffer
		ByteBuffer valueBuffer = localBufferValue.get();
		valueBuffer.clear();
		valueBuffer.put(value);
//		valueBuffer.put(value, 0, value.length);
//		valueBuffer.put(value, 0, value.length);
		valueBuffer.flip();
		log.info("buffer:{}", valueBuffer);

		if (offset != -1) {
			// key重复
			try {
				valueFileChannels[fileNo].write(valueBuffer, (long)(offset << SHIF_NUM));
			} catch (IOException e) {
				log.warn("write value file={} error", fileNo, e);
			}
		}else{
			try {
				// 获取要插入valueFile的offset，并将当前valueFile的offset加1
				offset = valueFileOffset[fileNo].getAndIncrement();
				// keyOffMap: 存储key和valueOff(插入的起始位置）
				keyOffMap.put(numKey, offset);

				// 将写入key和offset到keybuffer,并写入keyFile文件
				ByteBuffer keyOffBuffer = localBufferKey.get();
				keyOffBuffer.clear();
				keyOffBuffer.putLong(numKey).putInt(offset);
				keyOffBuffer.flip();

				// 写入(key,valueoffset)到keyFile文件
				keyFileChannel.write(keyOffBuffer, keyFileOffset.getAndAdd(KEY_OFF_LEN));

				// 写入value到valueFile文件
				log.info(" key:{} valueLength:{} offset:{} true:{}",  key, value.length, offset, (long)(offset << SHIF_NUM));
				valueFileChannels[fileNo].write(valueBuffer, (long)(offset << SHIF_NUM));

				flush(keyFileChannel);
				flush(valueFileChannels[fileNo]);
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
//		byte[] valByte = val.getValue();

		if (offset == -1) {
			// 不存在
			val.setValue(null);
		}else {
			// 存在
//			byte[] bytes = localValueBytes.get();
			try {
//				ByteBuffer valueBuffer = localBufferValue.get();
				ByteBuffer valueBuffer = ByteBuffer.allocate(VALUE_LEN);
				// 从valueFile中读取
				valueFileChannels[fileNo].read(valueBuffer, (long)(offset << SHIF_NUM));
//				log.info("key:{} get: value buffer:{}", key,valurBuffer);
//				log.info("key:{} get:   numKey:{} fileNo:{} offset:{} byte length:{}", key,  numKey, fileNo, offset, bytes.length);

				valueBuffer.flip();
//				log.info("key:{} get: value buffer:{}", key,valueBuffer);

				valueBuffer.get(valByte, 0, valueBuffer.limit());
//				valueBuffer.get(bytes, 0, VALUE_LEN);
//				valueBuffer.get(bytes, 0, valueBuffer.limit());
//				valueBuffer.get(bytes, 0, VALUE_LEN);
//				log.info("key:{} get fileNo:{} offset:{}  value:{} ", key, fileNo, offset, new String(bytes));
				log.info("key:{} get fileNo:{} offset:{}  value:{} ", key, fileNo, offset, valByte.length);

				valueBuffer.clear();
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
}
