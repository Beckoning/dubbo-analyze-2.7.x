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

package org.apache.dubbo.remoting.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * 该方法继承AbstractChannelBuffer，该类是基于 Java NIO中的ByteBuffer来实现相关的读写数据等操作。
 */
public class ByteBufferBackedChannelBuffer extends AbstractChannelBuffer {

    //ByteBuffer实例
    private final ByteBuffer buffer;
    //容量
    private final int capacity;

    /**
     * 上述就是该类的属性和构造函数，可以看到它有一个ByteBuffer类型的实例，并且capacity是buffer的剩余容量。
     */
    public ByteBufferBackedChannelBuffer(ByteBuffer buffer) {
        if (buffer == null) {
            throw new NullPointerException("buffer");
        }
        // 创建一个新的字节缓冲区，新缓冲区的大小将是此缓冲区的剩余容量
        this.buffer = buffer.slice();
        // 返回buffer的剩余容量
        capacity = buffer.remaining();
        // 设置写索引
        writerIndex(capacity);
    }

    public ByteBufferBackedChannelBuffer(ByteBufferBackedChannelBuffer buffer) {
        this.buffer = buffer.buffer;
        capacity = buffer.capacity;
        setIndex(buffer.readerIndex(), buffer.writerIndex());
    }

    @Override
    public ChannelBufferFactory factory() {
        // 判断缓冲区是否是直接缓冲区
        if (buffer.isDirect()) {
            return DirectChannelBufferFactory.getInstance();
        } else {
            return HeapChannelBufferFactory.getInstance();
        }
    }


    @Override
    public int capacity() {
        return capacity;
    }


    @Override
    public ChannelBuffer copy(int index, int length) {
        ByteBuffer src;
        try {
            // 创建一个缓存区，和buffer共享数据
            src = (ByteBuffer) buffer.duplicate().position(index).limit(index + length);
        } catch (IllegalArgumentException e) {
            throw new IndexOutOfBoundsException();
        }

        // 如果buffer是直接缓冲区，则分配一个直接缓冲区
        ByteBuffer dst = buffer.isDirect()
                ? ByteBuffer.allocateDirect(length)
                : ByteBuffer.allocate(length);
        dst.put(src);
        dst.clear();
        // 新建一个ByteBufferBackedChannelBuffer
        return new ByteBufferBackedChannelBuffer(dst);
    }

    /**
     * 获得某个位置的字节数据
     * @param index
     * @return
     */
    @Override
    public byte getByte(int index) {
        return buffer.get(index);
    }

    /**
     * 获得固定位置、固定长度的数据,并写入到dst字节数组中
     * @param index
     * @param dst
     * @param dstIndex the first index of the destination
     * @param length   the number of bytes to transfer
     */
    @Override
    public void getBytes(int index, byte[] dst, int dstIndex, int length) {
        // 创建一个共享数据的新缓冲区
        ByteBuffer data = buffer.duplicate();
        try {
            // 限制缓冲区大小为index + length，重置缓冲区位置为index
            data.limit(index + length).position(index);
        } catch (IllegalArgumentException e) {
            throw new IndexOutOfBoundsException();
        }
        // 将从dstIndex开始 length长度的字节数据写入到dst中
        data.get(dst, dstIndex, length);
    }


    /**
     * 获得固定位置,并写入到一个新的ByteBuffer中
     * @param index
     * @param dst
     */
    @Override
    public void getBytes(int index, ByteBuffer dst) {
        // 创建一个共享数据的新缓冲区
        ByteBuffer data = buffer.duplicate();
        // 取buffer的剩余容量-index 和 dst剩余容量的最小值
        int bytesToCopy = Math.min(capacity() - index, dst.remaining());
        try {
            // 限制data 的大小和重置位置到index
            data.limit(index + bytesToCopy).position(index);
        } catch (IllegalArgumentException e) {
            throw new IndexOutOfBoundsException();
        }
        // 传输剩余字节数
        dst.put(data);
    }


    @Override
    public void getBytes(int index, ChannelBuffer dst, int dstIndex, int length) {
        if (dst instanceof ByteBufferBackedChannelBuffer) {
            ByteBufferBackedChannelBuffer bbdst = (ByteBufferBackedChannelBuffer) dst;
            ByteBuffer data = bbdst.buffer.duplicate();

            data.limit(dstIndex + length).position(dstIndex);
            // 获取数据
            getBytes(index, data);
        } else if (buffer.hasArray()) {
            dst.setBytes(dstIndex, buffer.array(), index + buffer.arrayOffset(), length);
        } else {
            dst.setBytes(dstIndex, this, index, length);
        }
    }


    @Override
    public void getBytes(int index, OutputStream out, int length) throws IOException {
        if (length == 0) {
            return;
        }

        if (buffer.hasArray()) {
            out.write(
                    buffer.array(),
                    index + buffer.arrayOffset(),
                    length);
        } else {
            byte[] tmp = new byte[length];
            ((ByteBuffer) buffer.duplicate().position(index)).get(tmp);
            out.write(tmp);
        }
    }


    @Override
    public boolean isDirect() {
        return buffer.isDirect();
    }

    /**
     * 往固定位置写入字节数据
     * @param index
     * @param value
     */
    @Override
    public void setByte(int index, int value) {
        buffer.put(index, (byte) value);
    }

    /**
     * 从buffer的index开始，把src中从srcIndex位置开始的length长度数据写入到buffer
     * @param index
     * @param src
     * @param srcIndex
     * @param length
     */
    @Override
    public void setBytes(int index, byte[] src, int srcIndex, int length) {
        ByteBuffer data = buffer.duplicate();
        data.limit(index + length).position(index);
        data.put(src, srcIndex, length);
    }


    /**
     * 从buffer的index开始，把src的数据都写入buffer
     * @param index
     * @param src
     */
    @Override
    public void setBytes(int index, ByteBuffer src) {
        ByteBuffer data = buffer.duplicate();
        data.limit(index + src.remaining()).position(index);
        data.put(src);
    }


    @Override
    public void setBytes(int index, ChannelBuffer src, int srcIndex, int length) {
        if (src instanceof ByteBufferBackedChannelBuffer) {
            ByteBufferBackedChannelBuffer bbsrc = (ByteBufferBackedChannelBuffer) src;
            ByteBuffer data = bbsrc.buffer.duplicate();

            data.limit(srcIndex + length).position(srcIndex);
            setBytes(index, data);
        } else if (buffer.hasArray()) {
            src.getBytes(srcIndex, buffer.array(), index + buffer.arrayOffset(), length);
        } else {
            src.getBytes(srcIndex, this, index, length);
        }
    }


    @Override
    public ByteBuffer toByteBuffer(int index, int length) {
        if (index == 0 && length == capacity()) {
            return buffer.duplicate();
        } else {
            return ((ByteBuffer) buffer.duplicate().position(
                    index).limit(index + length)).slice();
        }
    }


    @Override
    public int setBytes(int index, InputStream in, int length) throws IOException {
        int readBytes = 0;

        if (buffer.hasArray()) {
            index += buffer.arrayOffset();
            do {
                int localReadBytes = in.read(buffer.array(), index, length);
                if (localReadBytes < 0) {
                    if (readBytes == 0) {
                        return -1;
                    } else {
                        break;
                    }
                }
                readBytes += localReadBytes;
                index += localReadBytes;
                length -= localReadBytes;
            } while (length > 0);
        } else {
            byte[] tmp = new byte[length];
            int i = 0;
            do {
                int localReadBytes = in.read(tmp, i, tmp.length - i);
                if (localReadBytes < 0) {
                    if (readBytes == 0) {
                        return -1;
                    } else {
                        break;
                    }
                }
                readBytes += localReadBytes;
                i += readBytes;
            } while (i < tmp.length);
            ((ByteBuffer) buffer.duplicate().position(index)).put(tmp);
        }

        return readBytes;
    }


    @Override
    public byte[] array() {
        return buffer.array();
    }


    @Override
    public boolean hasArray() {
        return buffer.hasArray();
    }


    @Override
    public int arrayOffset() {
        return buffer.arrayOffset();
    }
}
