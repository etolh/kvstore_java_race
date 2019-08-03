package com.huawei.hwcloud.tarus.kvstore.race.data;

import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.race.common.Constant;
import com.huawei.hwcloud.tarus.kvstore.race.common.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class ValueData {

    private final static Logger logger = LoggerFactory.getLogger(ValueData.class);

    private FileChannel valueFileChannel;
    private int fileLength;
    // KeyFileMMAP
    private MappedByteBuffer mmap;
    // 一行记录长度
    private int recordLength = Constant.VALUE_LEN;

    private ByteBuffer valueBuffer;
//    private byte[] valueBytes;

    /**
     * 获取ValueFile channel
     */
    public void init(String dir, final int thread_no, final int parNO) {

        String path = dir + File.separator + Utils.fillThreadNo(thread_no) + "_" + parNO + ".data";
        try {
            valueFileChannel = new RandomAccessFile(path, "rw").getChannel();
            this.fileLength = (int)valueFileChannel.size();
        } catch (IOException e) {
            logger.warn("init: can't open value file{} in thread {}", parNO, thread_no, e);
        }

        this.valueBuffer = Constant.localBufferValue.get();
//        this.valueBytes = Constant.localValueBytes.get();
    }

    /**
     * 读取offset偏移位置的value值
     */
    public byte[] read(long offset){
        long position = offset << Constant.SHIFT_NUM;
        // 从channel读
        try {
            valueFileChannel.read(valueBuffer, position);
        } catch (IOException e) {
            logger.warn("read: read from value file error", e);
        }
        // 写入到bytes
//        valueBuffer.get(valueBytes, 0, recordLength);
//        return valueBytes;
        return valueBuffer.array();         // buffer -> byte[]
    }

    /**
     * 将value按顺序追加到valueFile
     */
    public void write(final byte[] value){

        valueBuffer.put(value);
        valueBuffer.flip();
        try {
            valueFileChannel.write(valueBuffer, fileLength);
        } catch (IOException e) {
            logger.warn("set: write into value file error", e);
        }
        valueBuffer.clear();

        // 更新
        fileLength += recordLength;
    }

    public void close(){
        this.valueBuffer = null;
        if (this.valueFileChannel != null) {
            try {
                this.valueFileChannel.close();
            } catch (IOException e) {
                logger.warn("close: close value file channel error", e);
            }
        }
    }

    /**
     * 返回value文件偏移量(value个数)
     */
    public int getOffset(){
        return 0;
    }
}
