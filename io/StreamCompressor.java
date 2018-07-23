package com.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

//压缩从标准输入读取的数据，然后将其写到标准输出
//可通过测试（将StreamCompressor.class打包jar放在hadoop-2.7.4/share/hadoop/commom下）
//echo "hello world" | hadoop jar Practice.jar org.apache.hadoop.io.compress.GzipCodec | gunzip
//gunzip将字符解压缩并显示在标准输出

public class StreamCompressor {

    public static void main(String[] args) throws Exception {
        String codecClassname = args[0];
        Class<?> codecClass = Class.forName(codecClassname);

        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,conf);


        CompressionOutputStream out = codec.createOutputStream(System.out);
        IOUtils.copyBytes(System.in,out,4096,false);
        out.finish();//finish要求压缩方法完成到压缩数据流的写操作

    }
}
