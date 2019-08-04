package com.huawei.hwcloud.tarus.kvstore.race.data;

import com.huawei.hwcloud.tarus.kvstore.race.common.Constant;
import com.huawei.hwcloud.tarus.kvstore.race.common.Utils;
import moe.cnkirito.kdio.DirectIOLib;
import moe.cnkirito.kdio.DirectIOUtils;
import moe.cnkirito.kdio.DirectRandomAccessFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.huawei.hwcloud.tarus.kvstore.race.common.UnsafeUtils.UNSAFE;

public class ValueData {

    private final static Logger logger = LoggerFactory.getLogger(ValueData.class);
    private final int SHIFT_NUM = Constant.SHIFT_NUM;

    private FileChannel valueFileChannel;
    private int fileLength;
    private int offset;

    private static ThreadLocal<ByteBuffer> bufferThreadLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(Constant.VALUE_LEN));
    private static ThreadLocal<byte[]> byteArrayThreadLocal = ThreadLocal.withInitial(() -> new byte[Constant.VALUE_LEN]);
    // 一行记录长度
    private int recordLength = Constant.VALUE_LEN;

    // Direct IO
    public static DirectIOLib directIOLib = DirectIOLib.getLibForPath("/");
    private DirectRandomAccessFile directRandomAccessFile;
    private ByteBuffer writeBuffer;
    private int writePosition;
    private long writeBufferAddress;

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

        if (DirectIOLib.binit) {
            try {
                directRandomAccessFile = new DirectRandomAccessFile(new File(path), "rw");
                this.writeBuffer = DirectIOUtils.allocateForDirectIO(directIOLib, recordLength * 4);
                this.writePosition = 0;
                this.writeBufferAddress =((DirectBuffer)writeBuffer).address();
            } catch (IOException e) {
                logger.warn("init: can't open directIO value file{} in thread {}", parNO, thread_no, e);
            }
        }

    }

    /**
     * 读取offset偏移位置的value值
     */
    public byte[] read(long offset){

        ByteBuffer valueBuffer = bufferThreadLocal.get();
        byte[] valueBytes = byteArrayThreadLocal.get();
        long position = offset << SHIFT_NUM;

        if (directIOLib.binit)
        {
            try {
                directRandomAccessFile.read(valueBuffer, position);
            } catch (IOException e) {
                logger.warn("read: read from value file error", e);
            }
        }
        else {
            // 从channel读
            try {
                int len = valueFileChannel.read(valueBuffer, position);
//            System.out.println("read:"+ len);
            } catch (IOException e) {
                logger.warn("read: read from value file error", e);
            }
        }

        valueBuffer.flip();
        // 写入到bytes
        valueBuffer.get(valueBytes, 0, recordLength);
        valueBuffer.clear();
        return valueBytes;
    }

    /**
     * 将value按顺序追加到valueFile
     */
    public void write(final byte[] value){

        if (DirectIOLib.binit)
        {
            // 拷贝value到writeBuffer中，writeBufferAddress是writeBuffer地址
            UNSAFE.copyMemory(value, 16, null, writeBufferAddress + writePosition * recordLength, recordLength );
            this.writePosition++;
            if (writePosition >= 4){
                this.writeBuffer.position(0);
                this.writeBuffer.limit(writePosition * recordLength);
                try {
                    directRandomAccessFile.write(writeBuffer, fileLength);
                } catch (IOException e) {
                    logger.warn("valueData: valueBuffer:{} offset:{} valueLength:{}",  writeBuffer, offset, value.length);
                }
            }

            // 更新
            fileLength += writePosition * recordLength;
            offset += writePosition;
            writePosition = 0;
        }
        else {
            ByteBuffer valueBuffer = bufferThreadLocal.get();
            try {
                valueBuffer.put(value, 0, value.length);
            } catch (BufferOverflowException e) {
                logger.warn("valueData: valueBuffer:{} offset:{} valueLength:{}", valueBuffer, offset, value.length);
            }
            valueBuffer.flip();
            try {
                valueFileChannel.write(valueBuffer, fileLength);
                valueBuffer.clear();
//            System.out.println("write:"+ len);
            } catch (IOException e) {
                logger.warn("set: write into value file error", e);
            }

            // 更新
            fileLength += recordLength;
            offset += 1;
        }
    }

    public void close() {
        try {
            if (writePosition > 0) {
                this.writeBuffer.position(0);
                this.writeBuffer.limit(writePosition * recordLength);
                if (DirectIOLib.binit) {
                    this.directRandomAccessFile.write(writeBuffer, this.fileLength);
                } else {
                    this.valueFileChannel.write(this.writeBuffer);
                }

                this.fileLength += recordLength * writePosition;
                offset += writePosition;
                writePosition = 0;
            }
            if (this.directRandomAccessFile != null) {
                this.directRandomAccessFile.close();
            }
        }catch (IOException e) {
            logger.warn("close: close value file channel error", e);
        }

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
