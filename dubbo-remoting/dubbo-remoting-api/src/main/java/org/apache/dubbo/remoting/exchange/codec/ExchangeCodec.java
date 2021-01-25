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
package org.apache.dubbo.remoting.exchange.codec;

import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.io.Bytes;
import org.apache.dubbo.common.io.StreamUtils;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.Cleanable;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.serialize.ObjectOutput;
import org.apache.dubbo.common.serialize.Serialization;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;
import org.apache.dubbo.remoting.buffer.ChannelBufferInputStream;
import org.apache.dubbo.remoting.buffer.ChannelBufferOutputStream;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.DefaultFuture;
import org.apache.dubbo.remoting.telnet.codec.TelnetCodec;
import org.apache.dubbo.remoting.transport.CodecSupport;
import org.apache.dubbo.remoting.transport.ExceedPayloadLimitException;

import java.io.IOException;
import java.io.InputStream;

/**
 * ExchangeCodec.
 * 该类继承了TelnetCodec，是信息交换编解码器。
 * 在本文的开头，我就写到，dubbo将一条消息分成了协议头和协议体，用来解决粘包拆包问题 但是头跟体在编解码上有区别
 */
public class ExchangeCodec extends TelnetCodec {

    //可以看到 MAGIC是个固定的值，用来判断是不是dubbo协议的数据包，并且MAGIC_LOW和MAGIC_HIGH分别是MAGIC的低位和高位。

    // 协议头长度 header length.
    protected static final int HEADER_LENGTH = 16;
    // MAGIC二进制 magic header.
    //1101101010111011，十进制：55995
    protected static final short MAGIC = (short) 0xdabb;
    //Magic High 也就是0-7位：11011010
    protected static final byte MAGIC_HIGH = Bytes.short2bytes(MAGIC)[0];
    //Magic Low  8-15位 ：10111011
    protected static final byte MAGIC_LOW = Bytes.short2bytes(MAGIC)[1];
    // message flag.
    //128 二进制：10000000
    protected static final byte FLAG_REQUEST = (byte) 0x80;
    //64 二进制：1000000
    protected static final byte FLAG_TWOWAY = (byte) 0x40;
    //32 二进制：100000s
    protected static final byte FLAG_EVENT = (byte) 0x20;
    //31 二进制：11111
    protected static final int SERIALIZATION_MASK = 0x1f;
    private static final Logger logger = LoggerFactory.getLogger(ExchangeCodec.class);

    public Short getMagicCode() {
        return MAGIC;
    }

    /**
     * 该方法是根据消息的类型来分别进行编码，分为三种情况：Request类型、Response类型以及其他
     * @param channel
     * @param buffer
     * @param msg
     * @throws IOException
     */
    @Override
    public void encode(Channel channel, ChannelBuffer buffer, Object msg) throws IOException {
        //对请求信息进行编码
        if (msg instanceof Request) {
            encodeRequest(channel, buffer, (Request) msg);
        } else if (msg instanceof Response) {
            //对响应信息进行编码
            encodeResponse(channel, buffer, (Response) msg);
        } else {
            //其他信息进行编码
            // 直接让父类( Telnet ) 处理，目前是 Telnet 命令的结果。
            super.encode(channel, buffer, msg);
        }
    }

    @Override
    public Object decode(Channel channel, ChannelBuffer buffer) throws IOException {
        //将dubbo协议头读取到数组header
        int readable = buffer.readableBytes();
        // 读取前16字节的协议头数据，如果数据不满16字节，则读取全部
        byte[] header = new byte[Math.min(readable, HEADER_LENGTH)];
        buffer.readBytes(header);
        //解析dubbo协议数据部分
        return decode(channel, buffer, readable, header);
    }

    /**
     * 该方法就是解码前的一些核对过程，包括检测是否为dubbo协议，是否有拆包现象等，具体的解码在decodeBody方法。
     * @param channel
     * @param buffer
     * @param readable
     * @param header
     * @return
     * @throws IOException
     */
    @Override
    protected Object decode(Channel channel, ChannelBuffer buffer, int readable, byte[] header) throws IOException {
        // check magic number.
        //1.1 检查魔数，确定为dubbo协议帧
        if (readable > 0 && header[0] != MAGIC_HIGH
                || readable > 1 && header[1] != MAGIC_LOW) {
            int length = header.length;
            // 将 buffer 完全复制到 `header` 数组中
            if (header.length < readable) {
                header = Bytes.copyOf(header, readable);
                buffer.readBytes(header, length, readable - length);
            }
            for (int i = 1; i < header.length - 1; i++) {
                if (header[i] == MAGIC_HIGH && header[i + 1] == MAGIC_LOW) {
                    buffer.readerIndex(buffer.readerIndex() - header.length + i);
                    header = Bytes.copyOf(header, i);
                    break;
                }
            }
            return super.decode(channel, buffer, readable, header);
        }
        //1.2 检查是否读取了一个完整的dubbo协议头
        // check length.
        // Header 长度不够，返回需要更多的输入，解决拆包现象
        if (readable < HEADER_LENGTH) {
            return DecodeResult.NEED_MORE_INPUT;
        }

        // get data length.
        //1.3 从协议头的最后四个字节读取协议数据部分大小
        int len = Bytes.bytes2int(header, 12);
        // 检查信息头长度
        checkPayload(channel, len);
        //1.4 如果遇到半包问题，直接返回
        int tt = len + HEADER_LENGTH;
        // 总长度不够，返回需要更多的输入，解决拆包现象
        if (readable < tt) {
            return DecodeResult.NEED_MORE_INPUT;
        }

        // limit input stream.
        //1.5 解析协议数据部分
        ChannelBufferInputStream is = new ChannelBufferInputStream(buffer, len);

        try {
            // 对body反序列化
            return decodeBody(channel, is, header);
        } finally {
            // 如果不可用
            if (is.available() > 0) {
                try {
                    // 打印错误日志
                    if (logger.isWarnEnabled()) {
                        logger.warn("Skip input stream " + is.available());
                    }
                    // 跳过未读完的流
                    StreamUtils.skipUnusedStream(is);
                } catch (IOException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * 该方法就是解码的过程，并且对协议头和协议体分开解码，协议头编码是做或运算，而解码则是做并运算，
     * 协议体用反序列化的方式解码，同样也是分为了Request类型、Response类型进行解码。
     * @param channel
     * @param is
     * @param header
     * @return
     * @throws IOException
     */
    //问题：解决粘包、半包问题  使用自定义协议 header+body的方式来解决粘包、半包问题 其中header中记录了body的大小 通过这种方式便于协议的升级
    protected Object decodeBody(Channel channel, InputStream is, byte[] header) throws IOException {
        // 1.5.1 解析请求类型和消费端序列化的类型
        byte flag = header[2], proto = (byte) (flag & SERIALIZATION_MASK);
        // get request id.
        // 1.5.2 解析请求ID
        long id = Bytes.bytes2long(header, 4);
        //1.5.3 解码响应     // 如果第16位为0，则说明是响应
        if ((flag & FLAG_REQUEST) == 0) {
            // decode response.
            Response res = new Response(id);
            //1.5.3.1 事件类型
            if ((flag & FLAG_EVENT) != 0) {
                res.setEvent(true);
            }
            // get status.
            //1.5.3.2 获取响应码
            byte status = header[3];
            res.setStatus(status);
            try {
                //1.5.3.3 使用消费端序列化一致的反序列化类型对数据部分进行解码
                ObjectInput in = CodecSupport.deserialize(channel.getUrl(), is, proto);
                // 如果响应是成功的
                if (status == Response.OK) {
                    Object data;
                    //1.5.3.3.1 解码心跳数据
                    if (res.isHeartbeat()) {
                        // 如果是心跳事件，则心跳事件的解码
                        data = decodeHeartbeatData(channel, in);
                    } else if (res.isEvent()) {//解码事件
                        // 否则执行普通解码
                        data = decodeEventData(channel, in);
                    } else {
                        //解码响应信息
                        data = decodeResponseData(channel, in, getRequestData(id));
                    }
                    res.setResult(data);
                } else {
                    res.setErrorMessage(in.readUTF());
                }
            } catch (Throwable t) {
                res.setStatus(Response.CLIENT_ERROR);
                res.setErrorMessage(StringUtils.toString(t));
            }
            return res;
        } else {
            // decode request.
            //1.5.4 请求类型为请求类型
            Request req = new Request(id);
            // 设置版本号
            req.setVersion(Version.getProtocolVersion());
            // 如果第17位不为0，则是双向
            req.setTwoWay((flag & FLAG_TWOWAY) != 0);
            // 如果18位不为0，事件
            if ((flag & FLAG_EVENT) != 0) {
                req.setEvent(true);
            }
            try {
                //1.5.4.1 使用与消费端序列化一致的反序列化类型对数据部分进行解码
                ObjectInput in = CodecSupport.deserialize(channel.getUrl(), is, proto);
                Object data;
                if (req.isHeartbeat()) {//心跳数据
                    data = decodeHeartbeatData(channel, in);
                } else if (req.isEvent()) {//事件
                    data = decodeEventData(channel, in);
                } else {//请求数据
                    // 否则，用普通解码
                    data = decodeRequestData(channel, in);
                }
                // 把重新设置请求数据
                req.setData(data);
            } catch (Throwable t) {
                // bad request
                // 设置是异常请求
                req.setBroken(true);
                req.setData(t);
            }
            return req;
        }
    }

    protected Object getRequestData(long id) {
        DefaultFuture future = DefaultFuture.getFuture(id);
        if (future == null) {
            return null;
        }
        Request req = future.getRequest();
        if (req == null) {
            return null;
        }
        return req.getData();
    }

    /**
     * 该方法是对Request类型的消息进行编码，仔细阅读上述我写的注解，结合协议头各个位数的含义，
     * 好好品味我举的例子。享受二进制位运算带来的快乐，也可以看到前半部分逻辑是对协议头的编码，后面还有对body值的序列化
     * @param channel
     * @param buffer
     * @param req
     * @throws IOException
     */
    protected void encodeRequest(Channel channel, ChannelBuffer buffer, Request req) throws IOException {
        //1.1 获取序列化拓展类实现
        Serialization serialization = getSerialization(channel);
        // header.
        //1.2 创建dubbo协议拓展头节点数组，HEADER_LENGTH
        byte[] header = new byte[HEADER_LENGTH];
        // set magic number.
        //1.3 把魔数0xdabb 写入协议头
        // 设置前16位数据，也就是设置header[0]和header[1]的数据为Magic High和Magic Low
        Bytes.short2bytes(MAGIC, header);

        // set request and serialization flag.
        // 1.4 设置请求头类型与序列化类型，标记到协议头
        // 16-23位为serialization编号，用到或运算10000000|serialization编号，例如serialization编号为11111，则为00011111
        header[2] = (byte) (FLAG_REQUEST | serialization.getContentTypeId());

        // 继续上面的例子，00011111|1000000 = 01011111
        if (req.isTwoWay()) {
            header[2] |= FLAG_TWOWAY;
        }
        // 继续上面的例子，01011111|100000 = 011 11111 可以看到011代表请求标记、双向、是事件，这样就设置了16、17、18位，后面19-23位是Serialization 编号
        if (req.isEvent()) {
            header[2] |= FLAG_EVENT;
        }

        // set request id.
        //1.5 将请求ID设置到协议头
        Bytes.long2bytes(req.getId(), header, 4);

        // encode request data.
        //1.6 使用代码1.1 获取的序列化方式对象数据部分进行编码，并把协议数据部分写入缓存
        // // 编码 `Request.data` 到 Body ，并写入到 Buffer
        int savedWriteIndex = buffer.writerIndex();
        buffer.writerIndex(savedWriteIndex + HEADER_LENGTH);
        ChannelBufferOutputStream bos = new ChannelBufferOutputStream(buffer);
        // 对body数据序列化
        ObjectOutput out = serialization.serialize(channel.getUrl(), bos);
        // 如果该请求是事件
        if (req.isEvent()) {
            // 特殊事件编码
            encodeEventData(channel, out, req.getData());
        } else {
            // 正常请求编码
            encodeRequestData(channel, out, req.getData(), req.getVersion());
        }
        //1.7 刷新缓存     // 释放资源
        out.flushBuffer();
        if (out instanceof Cleanable) {
            ((Cleanable) out).cleanup();
        }
        bos.flush();
        bos.close();
        //1.8 检查payload（协议数据部分）是否合法
        int len = bos.writtenBytes();
        //默认消息体的数据为8M
        checkPayload(channel, len);
        // 设置96-127位：Body值
        Bytes.int2bytes(len, header, 12);

        // write
        //1.9 将协议头写入缓存
        buffer.writerIndex(savedWriteIndex);
        buffer.writeBytes(header); // write header.
        buffer.writerIndex(savedWriteIndex + HEADER_LENGTH + len);
    }

    /**
     * 该方法是对Response类型的消息进行编码，该方法里面我没有举例子演示如何进行编码，不过过程跟encodeRequest类似。
     * @param channel
     * @param buffer
     * @param res
     * @throws IOException
     */
    protected void encodeResponse(Channel channel, ChannelBuffer buffer, Response res) throws IOException {
        int savedWriteIndex = buffer.writerIndex();
        try {
            //1.1 获取序列化拓展实现
            Serialization serialization = getSerialization(channel);
            // header.
            //1.2 创建 dubbo协议拓展头节点数组，HEADER_LENGTH为16
            byte[] header = new byte[HEADER_LENGTH];
            // set magic number.
            //1.3 把魔数0xdabb 写入协议头   // 设置前0-15位为魔数
            Bytes.short2bytes(MAGIC, header);
            // set request and serialization flag.
            //1.4 设置请求序列与序列化类型，标记到协议头   // 设置响应标志和序列化id
            header[2] = serialization.getContentTypeId();
            // 如果是心跳事件，则设置第18位为事件
            if (res.isHeartbeat()) {
                header[2] |= FLAG_EVENT;
            }
            // set response status.
            //1.5 设置响应类型到第4字节  // 设置24-31位为状态码
            byte status = res.getStatus();
            header[3] = status;
            // set request id.
            //1.6 将请求id设置到协议头  // 设置32-95位为请求id
            Bytes.long2bytes(res.getId(), header, 4);
            //1.7 使用代码1.1 获取的序列化方式对象数据部分进行编码，并把协议数据部分写入缓存
            buffer.writerIndex(savedWriteIndex + HEADER_LENGTH);
            ChannelBufferOutputStream bos = new ChannelBufferOutputStream(buffer);
            // 对body进行序列化
            ObjectOutput out = serialization.serialize(channel.getUrl(), bos);
            // encode response data or error message.
            if (status == Response.OK) {
                // 对心跳事件编码
                if (res.isHeartbeat()) {
                    encodeEventData(channel, out, res.getResult());
                } else {
                    // 对普通响应编码
                    encodeResponseData(channel, out, res.getResult(), res.getVersion());
                }
            } else {
                out.writeUTF(res.getErrorMessage());
            }
            //1.8 刷新缓存
            out.flushBuffer();
            if (out instanceof Cleanable) {
                ((Cleanable) out).cleanup();
            }
            bos.flush();
            bos.close();
            // 1.9 检查payload（协议数据部分）是否合法
            int len = bos.writtenBytes();
            checkPayload(channel, len);
            Bytes.int2bytes(len, header, 12);
            // write
            //1.10 将协议头写入缓存
            buffer.writerIndex(savedWriteIndex);
            buffer.writeBytes(header); // write header.
            buffer.writerIndex(savedWriteIndex + HEADER_LENGTH + len);
        } catch (Throwable t) {
            // clear buffer
            buffer.writerIndex(savedWriteIndex);
            // send error message to Consumer, otherwise, Consumer will wait till timeout.
            //如果在写入数据失败，则返回响应格式错误的返回码
            if (!res.isEvent() && res.getStatus() != Response.BAD_RESPONSE) {
                Response r = new Response(res.getId(), res.getVersion());
                r.setStatus(Response.BAD_RESPONSE);

                if (t instanceof ExceedPayloadLimitException) {
                    logger.warn(t.getMessage(), t);
                    try {
                        r.setErrorMessage(t.getMessage());
                        // 发送响应
                        channel.send(r);
                        return;
                    } catch (RemotingException e) {
                        logger.warn("Failed to send bad_response info back: " + t.getMessage() + ", cause: " + e.getMessage(), e);
                    }
                } else {
                    // FIXME log error message in Codec and handle in caught() of IoHanndler?
                    logger.warn("Fail to encode response: " + res + ", send bad_response info instead, cause: " + t.getMessage(), t);
                    try {
                        r.setErrorMessage("Failed to send response: " + res + ", cause: " + StringUtils.toString(t));
                        channel.send(r);
                        return;
                    } catch (RemotingException e) {
                        logger.warn("Failed to send bad_response info back: " + res + ", cause: " + e.getMessage(), e);
                    }
                }
            }

            // Rethrow exception
            if (t instanceof IOException) {
                throw (IOException) t;
            } else if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else if (t instanceof Error) {
                throw (Error) t;
            } else {
                throw new RuntimeException(t.getMessage(), t);
            }
        }
    }

    @Override
    protected Object decodeData(ObjectInput in) throws IOException {
        return decodeRequestData(in);
    }

    @Deprecated
    protected Object decodeHeartbeatData(ObjectInput in) throws IOException {
        return decodeEventData(null, in);
    }

    protected Object decodeRequestData(ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    protected Object decodeResponseData(ObjectInput in) throws IOException {
        try {
            return in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Read object failed.", e));
        }
    }

    @Override
    protected void encodeData(ObjectOutput out, Object data) throws IOException {
        encodeRequestData(out, data);
    }

    private void encodeEventData(ObjectOutput out, Object data) throws IOException {
        out.writeEvent(data);
    }

    @Deprecated
    protected void encodeHeartbeatData(ObjectOutput out, Object data) throws IOException {
        encodeEventData(out, data);
    }

    protected void encodeRequestData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    protected void encodeResponseData(ObjectOutput out, Object data) throws IOException {
        out.writeObject(data);
    }

    @Override
    protected Object decodeData(Channel channel, ObjectInput in) throws IOException {
        return decodeRequestData(channel, in);
    }

    protected Object decodeEventData(Channel channel, ObjectInput in) throws IOException {
        try {
            return in.readEvent();
        } catch (IOException | ClassNotFoundException e) {
            throw new IOException(StringUtils.toString("Decode dubbo protocol event failed.", e));
        }
    }

    @Deprecated
    protected Object decodeHeartbeatData(Channel channel, ObjectInput in) throws IOException {
        return decodeEventData(channel, in);
    }

    protected Object decodeRequestData(Channel channel, ObjectInput in) throws IOException {
        return decodeRequestData(in);
    }

    protected Object decodeResponseData(Channel channel, ObjectInput in) throws IOException {
        return decodeResponseData(in);
    }

    protected Object decodeResponseData(Channel channel, ObjectInput in, Object requestData) throws IOException {
        return decodeResponseData(channel, in);
    }

    @Override
    protected void encodeData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(channel, out, data);
    }

    private void encodeEventData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeEventData(out, data);
    }

    @Deprecated
    protected void encodeHeartbeatData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeHeartbeatData(out, data);
    }

    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(out, data);
    }

    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeResponseData(out, data);
    }

    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        encodeRequestData(out, data);
    }

    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        encodeResponseData(out, data);
    }


}
