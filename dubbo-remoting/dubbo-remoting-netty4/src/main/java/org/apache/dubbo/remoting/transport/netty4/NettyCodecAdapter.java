/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.remoting.transport.netty4;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;
import java.util.List;

/**
 * NettyCodecAdapter.
 */
final public class NettyCodecAdapter {
    //编码器
    private final ChannelHandler encoder = new InternalEncoder();
    //解码器
    private final ChannelHandler decoder = new InternalDecoder();
    //编解码器
    private final Codec2 codec;
    // url对象
    private final URL url;
    //通道处理器
    private final org.apache.dubbo.remoting.ChannelHandler handler;

    public NettyCodecAdapter(Codec2 codec, URL url, org.apache.dubbo.remoting.ChannelHandler handler) {
        this.codec = codec;
        this.url = url;
        this.handler = handler;
    }

    public ChannelHandler getEncoder() {
        return encoder;
    }

    public ChannelHandler getDecoder() {
        return decoder;
    }

    /**
     * 该内部类是编码器的抽象，主要的编码还是调用了codec.encode。
     */
    private class InternalEncoder extends MessageToByteEncoder {

        @Override
        protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
            // 创建缓冲区
            org.apache.dubbo.remoting.buffer.ChannelBuffer buffer = new NettyBackedChannelBuffer(out);
            // 获得通道
            Channel ch = ctx.channel();
            // 获得netty通道
            NettyChannel channel = NettyChannel.getOrAddChannel(ch, url, handler);
            // 编码
            codec.encode(channel, buffer, msg);
        }
    }

    /**
     * 该内部类是解码器的抽象类，其中关键的是调用了codec.decode。
     */
    private class InternalDecoder extends ByteToMessageDecoder {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf input, List<Object> out) throws Exception {

            // 创建缓冲区
            ChannelBuffer message = new NettyBackedChannelBuffer(input);

            // 获得通道
            NettyChannel channel = NettyChannel.getOrAddChannel(ctx.channel(), url, handler);

            // decode object.
            do {
                //0.0 具体解析二进制dubbo协议帧对象
                int saveReaderIndex = message.readerIndex();
                //0.1 解析二进制数据为对象
                Object msg = codec.decode(channel, message);
                //0.2 如果返回是 NEED_MORE_INPUT 说明遇到了半包问题，重置缓存的读取下表
                if (msg == Codec2.DecodeResult.NEED_MORE_INPUT) {
                    message.readerIndex(saveReaderIndex);
                    break;
                } else {
                    //is it possible to go here ?
                    // 0.3 把解码成功的对象放到out列表  说明解析出来了一个完整的dubbo协议帧
                    if (saveReaderIndex == message.readerIndex()) {
                        throw new IOException("Decode without read data.");
                    }
                    if (msg != null) {
                        out.add(msg);
                    }
                }
            } while (message.readable());
        }
    }
}
