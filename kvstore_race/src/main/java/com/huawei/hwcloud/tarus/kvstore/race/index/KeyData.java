package com.huawei.hwcloud.tarus.kvstore.race.index;

import com.huawei.hwcloud.tarus.kvstore.race.common.Constant;
import com.huawei.hwcloud.tarus.kvstore.race.common.Utils;
import com.huawei.hwcloud.tarus.kvstore.race.data.ValueData;
import com.huawei.hwcloud.tarus.kvstore.race.index.map.HPPCMemoryMap;
import com.huawei.hwcloud.tarus.kvstore.race.index.map.MemoryMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class KeyData {

    private final static Logger logger = LoggerFactory.getLogger(KeyData.class);

    // 缓存key-->(par,off)
    private MemoryMap memoryMap;
    private FileChannel keyFileChannel;
//    private int indexSize;
    private int fileLength;
//    private int offset;
    private int parNO;
    // KeyFileMMAP
    private MappedByteBuffer mmap;
    // 一行记录长度
    private int recordLength = Constant.KEY_OFF_LEN;

    public ValueData getValueData() {
        return valueData;
    }

    public void setValueData(ValueData valueData) {
        this.valueData = valueData;
    }

    private ValueData valueData;

    public void init(String dir, final int thread_no, final int parNO, MemoryMap memoryMap) {

        String path = dir + File.separator + Utils.fillThreadNo(thread_no) + "_" + parNO + ".key";
        try {
            keyFileChannel = new RandomAccessFile(path, "rw").getChannel();
//            this.offset = (int)(keyFileChannel.size() / recordLength);
            // init：fileLength=0 无法写入
            this.mmap = keyFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constant.KV_NUMBER_PER_PAR * recordLength);
        } catch (IOException e) {
            logger.warn("init: open key file={} in thread={} error", parNO, thread_no, e);
        }

        this.fileLength = valueData.getOffset() * recordLength;
        this.parNO = parNO;
//        this.memoryMap = new HPPCMemoryMap(Constant.KV_NUMBER_PER_PAR, 0.99f);
        this.memoryMap = memoryMap;
//        logger.info("keyData init: parNo {} fileLength:{} offset:{}", parNO, fileLength, offset);
//        logger.info("keyData init: parNo {} fileLength:{}", parNO, fileLength);

    }

    /**
     * 将(numKey,parOff)按顺序写入到keyFile中
     */
    public void write(long numKey){

        int offset = valueData.getOffset();
        long parOff = Utils.combine(parNO, offset);
        mmap.position(fileLength);
        mmap.putLong(numKey).putLong(parOff);
        // 缓存映射
        memoryMap.insert(numKey, parOff);
        // 更新
//        logger.info("keyData write: key:{}  parNo {} offset:{}", numKey, parNO, offset);
        fileLength += recordLength;
//        offset += 1;
    }

    /**
     * 从map中获取key对应的parNo+offset
     */
    public long read(long numKey){
        return memoryMap.get(numKey);
    }

    public void close(){
        if (this.mmap != null) {
            try {
                keyFileChannel.close();
            } catch (IOException e) {
                logger.warn("close: close key file channel error", e);
            }
            Utils.clean(this.mmap);
            this.keyFileChannel = null;
            this.mmap = null;
        }
    }

    /**
     * 读取KeyFile记录,构建map缓存
     */
    public void load(){
        // mmap读取keyFile
//        logger.info("keyData load: parNo {} fileLength:{}", parNO, fileLength);
        int start = 0;
        while (start < fileLength) {
            start += recordLength;
            memoryMap.insert(mmap.getLong(), mmap.getLong());
        }
    }


//    public int getOffset(){
//        logger.info("keyData getOffset: parNo {} fileLength:{} offset:{}", parNO, fileLength, offset);
//        return offset;
//    }
}
