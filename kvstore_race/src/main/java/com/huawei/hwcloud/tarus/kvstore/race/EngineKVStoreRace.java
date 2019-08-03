package com.huawei.hwcloud.tarus.kvstore.race;

import com.carrotsearch.hppc.LongLongHashMap;
import com.huawei.hwcloud.tarus.kvstore.common.KVStoreRace;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import com.huawei.hwcloud.tarus.kvstore.race.common.Constant;
import com.huawei.hwcloud.tarus.kvstore.race.common.Utils;
import com.huawei.hwcloud.tarus.kvstore.race.data.ValueData;
import com.huawei.hwcloud.tarus.kvstore.race.index.KeyData;
import com.huawei.hwcloud.tarus.kvstore.race.index.map.HPPCMemoryMap;
import com.huawei.hwcloud.tarus.kvstore.race.index.map.MemoryMap;
import com.huawei.hwcloud.tarus.kvstore.race.partition.PartitionLayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class EngineKVStoreRace implements KVStoreRace {

	private static Logger log = LoggerFactory.getLogger(EngineKVStoreRace.class);
	private static final int MSG_NUMBER = Constant.MSG_NUMBER;
	private static final int PARTITION_COUNT = Constant.PARTITION_COUNT;
	private static final int KV_NUMBER_PER_PAR = Constant.KV_NUMBER_PER_PAR;
	private static final int THREAD_NUM = 16;

	// 全局分区管理、Map映射管理
	private AtomicInteger partitionNo = new AtomicInteger(0);
	private MemoryMap memoryMap;
	// new
//	private String filePath;
	private KeyData[] keyDatas;
	private ValueData[] valueDatas;

	@Override
	public boolean init(final String dir, final int file_size) throws KVSException {

		// 在dir父目录创建文件夹
		File dirParent = new File(dir).getParentFile();
		if (!dirParent.exists())
			dirParent.mkdirs();
//		this.filePath = dirParent.getPath();
		String filePath = dirParent.getPath();

		keyDatas = new KeyData[PARTITION_COUNT];
		valueDatas = new ValueData[PARTITION_COUNT];
		// 全局map
		this.memoryMap = new HPPCMemoryMap(MSG_NUMBER, 0.99f);

		for (int i = 0; i < PARTITION_COUNT; i++){
			keyDatas[i] = new KeyData();
			// keyFile使用全局map构建映射
			keyDatas[i].init(filePath, file_size, i, memoryMap);
			valueDatas[i] = new ValueData();
			valueDatas[i].init(filePath, file_size, i);
		}

		loadAllIndex();
		return true;
	}

	/**
	 * 加载keyFile,生成各自分区Map索引
	 */
	private void loadAllIndex() {
		int loadThreadNum = THREAD_NUM;
		CountDownLatch countDownLatch = new CountDownLatch(loadThreadNum);
		for (int i = 0; i < loadThreadNum; i++) {
			final int index = i;
			new Thread(() -> {
				for (int partition = 0; partition < PARTITION_COUNT; partition++) {
					if (partition % loadThreadNum == index) {
						keyDatas[partition].load();
					}
				}
				countDownLatch.countDown();
			}).start();
		}

		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			log.error("load index interrupted", e);
		}

		// 二次读取KeyFile时，根据分区是否已满，找到第一个未满的分区
		for (int i = 0; i < PARTITION_COUNT; i++) {
			int offset = keyDatas[i].getOffset();
			if (offset >= KV_NUMBER_PER_PAR){
				partitionNo.getAndIncrement();
			}
		}
	}

	private void increasePartition(int parNo){
		int offset = keyDatas[parNo].getOffset();
		if (offset >= KV_NUMBER_PER_PAR) {  // off >= 4000 当前分区已满，放到下一个分区
			partitionNo.incrementAndGet(); // 分区+1
			parNo = partitionNo.get();
		}
	}

	@Override
	public long set(final String key, final byte[] value) throws KVSException {

		long numKey = Long.parseLong(key);
		// 获取分区号
		int parNo = partitionNo.get();
		int offset = keyDatas[parNo].getOffset();
		if (offset >= KV_NUMBER_PER_PAR) {  	// off >= 4000 当前分区已满，放到下一个分区
			partitionNo.incrementAndGet(); 		// 分区+1
			parNo = partitionNo.get();
		}

		keyDatas[parNo].write(numKey);
		valueDatas[parNo].write(value);
		return 0;
	}

	@Override
	public long get(final String key, final Ref<byte[]> val) throws KVSException {
        long numKey = Long.parseLong(key);
        // 从全局Map获取parNo,off
		long partitionOff = memoryMap.get(numKey);

		if (partitionOff == -1) {
			val.setValue(null);
		}else {
			int[] coms = Utils.divide(partitionOff);
			int offset = coms[0];
			int parNo = coms[1];
			byte[] bytes = valueDatas[parNo].read(offset);
			val.setValue(bytes);
		}
		return 0;
	}

	@Override
	public void close() {
		if (keyDatas != null) {
			for (KeyData keyData : keyDatas){
				keyData.close();
			}
		}
		if (valueDatas != null) {
			for (ValueData valueData : valueDatas){
				valueData.close();
			}
		}
	}

	@Override
	public void flush() {

	}
}
