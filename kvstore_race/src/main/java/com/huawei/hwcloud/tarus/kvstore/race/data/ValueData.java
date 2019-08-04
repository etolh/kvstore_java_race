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
    private final int SHIFT_NUM = Constant.SHIFT_NUM;

    private FileChannel valueFileChannel;
    private int fileLength;
    private int offset;

    // KeyFileMMAP
    private MappedByteBuffer mmap;
    // 一行记录长度
    private int recordLength = Constant.VALUE_LEN;

    private ByteBuffer valueBuffer;
    private byte[] valueBytes;

    /**
     * 获取ValueFile channel
     */
    public void init(String dir, final int thread_no, final int parNO) {

        String path = dir + File.separator + Utils.fillThreadNo(thread_no) + "_" + parNO + ".data";
        try {
            valueFileChannel = new RandomAccessFile(path, "rw").getChannel();
            this.fileLength = (int)valueFileChannel.size();
            this.offset = (int)(valueFileChannel.size() >>> SHIFT_NUM);
        } catch (IOException e) {
            logger.warn("init: can't open value file{} in thread {}", parNO, thread_no, e);
        }

        this.valueBuffer = Constant.localBufferValue.get();
        this.valueBytes = Constant.localValueBytes.get();
    }

    /**
     * 读取offset偏移位置的value值
     */
    public byte[] read(long offset){

        long position = offset << SHIFT_NUM;
        // 从channel读
        try {
            int len = valueFileChannel.read(valueBuffer, position);
//            System.out.println("read:"+ len);
        } catch (IOException e) {
            logger.warn("read: read from value file error", e);
        }

        valueBuffer.flip();
        // 写入到bytes
        valueBuffer.get(valueBytes, 0, recordLength);
        valueBuffer.clear();
        return valueBytes;
//        return valueBuffer.array();         // buffer -> byte[]
    }

    /**
     * 将value按顺序追加到valueFile
     */
    public void write(final byte[] value){

        valueBuffer.put(value);
        valueBuffer.flip();
        try {
            valueFileChannel.write(valueBuffer, fileLength);
//            System.out.println("write:"+ len);
        } catch (IOException e) {
            logger.warn("set: write into value file error", e);
        }
        valueBuffer.clear();

        // 更新
        fileLength += recordLength;
        offset += 1;
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
     * 返回value文件偏移量(key或value个数)
     */
    public int getOffset(){
        return offset;
    }
}
