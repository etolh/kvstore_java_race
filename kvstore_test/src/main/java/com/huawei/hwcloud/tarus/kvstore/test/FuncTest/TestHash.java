package com.huawei.hwcloud.tarus.kvstore.test.FuncTest;

import com.huawei.hwcloud.tarus.kvstore.race.common.Utils;
import com.huawei.hwcloud.tarus.kvstore.util.BufferUtil;
import org.junit.jupiter.api.Test;

/**
 * @ClassName TestHash
 * @Description TODO
 * @Author lianghu
 * @Date 2019-07-28 17:07
 * @VERSION 1.0
 */
public class TestHash {

    @Test
    public void testHash(){

        // 直接基于key哈希
        long l1 = 12885411404L, l2 = 96086412L;
        System.out.println("hash2:"+Utils.fileHash(l1));
        System.out.println("hash2:"+Utils.valueFileHash2(l2));

        String sl1 = "12885411404", sl2 = "96086412";
        long numKey1 = Long.parseLong(sl1);
        long numKey2 = Long.parseLong(sl2);
        int paritionNo1 = Utils.fileHash(numKey1);
        int paritionNo2 = Utils.fileHash(numKey2);
        System.out.println("hash2:"+paritionNo1);
        System.out.println("hash2:"+paritionNo2);

        byte[] sb1 = BufferUtil.stringToBytes(sl1);
        for (int i = 0; i < sb1.length; i++)
            System.out.println(sb1[i]);

        System.out.println("****************");
        byte[] lsb1 = Utils.long2bytes(numKey1);
        for (int i = 0; i < lsb1.length; i++)
            System.out.println(lsb1[i]);

        System.out.println("****************");
        long sl11 = 12885411405L;
        byte[] lsb2 = Utils.long2bytes(sl11);
        for (int i = 0; i < lsb2.length; i++)
            System.out.println(lsb2[i]);



        System.out.println("*****************************");
        for (long i = 10009000; i <= 10021000; i++){
//            byte[] ls = Utils.long2bytes(i);
            System.out.println("Par: "+ Utils.fileHash2(i));
        }
        /*
        for (int i = 10000900; i <= 10001000; i++){
            String key = buildKey(i);
            long numKey = Long.parseLong(key);
            System.out.println(numKey);
//            System.out.println("hash1:"+Utils.valueFileHash(numKey));
            System.out.println("hash2:"+Utils.valueFileHash2(numKey));

        }
        */
    }

    @Test
    public void testKey() {
//        long n1 =12885358397L, n2 = 12885358497L;
        long n1 =0, n2 = 12885358497L;
//        String sl1 = "12885411404", sl2 = "12885411404";

//        byte[] ls = Utils.long2bytes(n1);
//        byte[] ls2 = Utils.long2bytes(n2);
//        System.out.println("Par: "+ Utils.getPartition(ls));
//        System.out.println("Par: "+ Utils.getPartition(ls2));

//        String n1s = "12885358397", n2s = "30020841000000472";
//        byte[] lss = BufferUtil.stringToBytes(n1s);
//        byte[] lss2 = BufferUtil.stringToBytes(n1s);
//        System.out.println("Par: "+ Utils.getPartition(lss));
//        System.out.println("Par: "+ Utils.getPartition(lss2));
//
//        for (int i = 0; i < ls.length; i++)
//            System.out.println("ls"+i+"-"+ls[i]);
//        long n1 = Long.parseLong(sl1);
        System.out.println(Utils.getPartition2(1L));

        for (long k = n1; k <= n1 + 10000; k++) {
//            byte[] ls = Utils.long2bytes(k);
//            String s = String.valueOf(k);
//            byte[] ls = BufferUtil.stringToBytes(s);
//            byte[] ls = s.getBytes();

//            for (int i = 0; i < ls.length; i++)
//                System.out.print(ls[i]+" ");
//            System.out.println();
            //                System.out.print(ls[i]+" ");
            System.out.println("i:"+(k-n1)+"  Par:"+ Utils.getPartition2(k));


        }



//        for (long i = n1; i <= n2; i++){
//            byte[] ls = Utils.long2bytes(i);
//            System.out.println("i:"+(i-n1)+"  Par:"+ Utils.getPartition(ls));
//        }

        byte b1 = (byte)1;
        byte b2 = 2;
//        String binaryString = Byte;

//        System.out.println(binaryString);
    }



    @Test
    public void testBitOpr() {
        byte a = 'a';
        System.out.println(a);
        System.out.println(a & 0xff);
    }

    @Test
    public void testBitDiv() {
        System.out.println("*****************************");
        // key 8B 32位 若前10位有数字，也是2^54,即要在15位以上，而测试中key一般在10位左右，因此不应该用前两位
        long n1 =12885358397L, n2 = 30020841000000472L;
        byte[] ls = Utils.long2bytes(n1);
        byte[] ls2 = Utils.long2bytes(n2);
        System.out.println("Par: "+ Utils.getPartition(ls));
        System.out.println("Par: "+ Utils.getPartition(ls2));

        String n1s = "12885358397", n2s = "30020841000000472";
        byte[] lss = BufferUtil.stringToBytes(n1s);
        byte[] lss2 = BufferUtil.stringToBytes(n1s);
        System.out.println("Par: "+ Utils.getPartition(lss));
        System.out.println("Par: "+ Utils.getPartition(lss2));

        for (int i = 0; i < ls2.length; i++)
            System.out.println(ls2[i]);
    }

    @Test
    public void testBitDiv2() {
        long begin = 12885411404L;
        for (int i = 1; i <= 1000000; i++){
            String key = buildKey(begin+i);
//            long numKey = Long.parseLong(key);
            System.out.println("*****************************");
            byte[] bytes = BufferUtil.stringToBytes(key);
//            byte[] bytes = Utils.long2bytes(begin+i);
            int fileNo = Utils.getPartition(bytes);
            System.out.println("key:"+key+" fileNo:"+fileNo);

//            for (int j = 0; j < bytes.length; j++)
//                System.out.print(bytes[j] + " ");
//            System.out.println();
        }
    }

    @Test
    public void testParitition(){
        long begin = 12885411404L, num = 10000, div = 4096;
        long bPar = begin / div;
//        System.out.println(begin / 4096 - bPar);
//        System.out.println((begin >>> 12) - bPar);
        // 位操作必须()
        for (long i = begin; i <= begin + num; i++){
            System.out.println("i:"+(i-begin)+" par:"+(i/div-bPar));
            System.out.println("bit i:"+(i-begin)+" par:"+((i>>>12)-bPar));
        }
    }
    @Test
    public void testCombine() {
//        long pos = Utils.combine(12, 234);
        long pos = 0L;
        //268435455
        for (int n:Utils.divide(pos)){
            System.out.println(n);
        }
    }
    private final String buildKey(final long i) {
        return String.format("%d", i);
    }

}
