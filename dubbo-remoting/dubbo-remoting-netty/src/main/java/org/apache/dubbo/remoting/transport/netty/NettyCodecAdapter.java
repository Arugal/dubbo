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
package org.apache.dubbo.remoting.transport.netty;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.buffer.DynamicChannelBuffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import java.io.IOException;

/**
 * NettyCodecAdapter.
 */
final class NettyCodecAdapter {

    private final ChannelHandler encoder = new InternalEncoder();

    private final ChannelHandler decoder = new InternalDecoder();

    private final Codec2 codec;

    private final URL url;

    private final int bufferSize;

    private final org.apache.dubbo.remoting.ChannelHandler handler;

    public NettyCodecAdapter(Codec2 codec, URL url, org.apache.dubbo.remoting.ChannelHandler handler) {
        this.codec = codec;
        this.url = url;
        this.handler = handler;
        int b = url.getPositiveParameter(Constants.BUFFER_KEY, Constants.DEFAULT_BUFFER_SIZE);
        this.bufferSize = b >= Constants.MIN_BUFFER_SIZE && b <= Constants.MAX_BUFFER_SIZE ? b : Constants.DEFAULT_BUFFER_SIZE;
    }

    public ChannelHandler getEncoder() {
        return encoder;
    }

    public ChannelHandler getDecoder() {
        return decoder;
    }

    @Sharable
    private class InternalEncoder extends OneToOneEncoder {

        @Override
        protected Object encode(ChannelHandlerContext ctx, Channel ch, Object msg) throws Exception {
            org.apache.dubbo.remoting.buffer.ChannelBuffer buffer =
                    org.apache.dubbo.remoting.buffer.ChannelBuffers.dynamicBuffer(1024);
            NettyChannel channel = NettyChannel.getOrAddChannel(ch, url, handler);
            try {
                codec.encode(channel, buffer, msg);
            } finally {
                NettyChannel.removeChannelIfDisconnected(ch);
            }
            return ChannelBuffers.wrappedBuffer(buffer.toByteBuffer());
        }
    }

    private class InternalDecoder extends SimpleChannelUpstreamHandler {

        private org.apache.dubbo.remoting.buffer.ChannelBuffer buffer =
                org.apache.dubbo.remoting.buffer.ChannelBuffers.EMPTY_BUFFER;

        /**
         * 这里需要 netty 的知识，继承 SimpleChannelUpstreamHandler, 表明它是进站的 handler
         * 所以进站的数据流，都会进过本 handler 对象，具体就是 messageReceived 方法
         * @param ctx
         * @param event
         * @throws Exception
         */
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) throws Exception {
            Object o = event.getMessage();
            if (!(o instanceof ChannelBuffer)) { // 这里 ChannelBuffer 是 netty 基于 jdk nio ByteBuffer 实现
                ctx.sendUpstream(event);
                return;
            }

            ChannelBuffer input = (ChannelBuffer) o;
            int readable = input.readableBytes(); // 到这里就是从 netty event 对象取数据的过程
            if (readable <= 0) { // 判断可读
                return;
            }

            org.apache.dubbo.remoting.buffer.ChannelBuffer message;
            if (buffer.readable()) {
                if (buffer instanceof DynamicChannelBuffer) {
                    buffer.writeBytes(input.toByteBuffer());
                    message = buffer;
                } else {
                    int size = buffer.readableBytes() + input.readableBytes();
                    message = org.apache.dubbo.remoting.buffer.ChannelBuffers.dynamicBuffer(
                            size > bufferSize ? size : bufferSize); // bufferSize 不指定是 8k, 这里表示最小 8k
                    message.writeBytes(buffer, buffer.readableBytes());
                    message.writeBytes(input.toByteBuffer()); // 把 netty 读到的字节流写入 message
                }
            } else {
                // 直接通过构造器，构造 message
                message = org.apache.dubbo.remoting.buffer.ChannelBuffers.wrappedBuffer(
                        input.toByteBuffer());
            }

            NettyChannel channel = NettyChannel.getOrAddChannel(ctx.getChannel(), url, handler);
            Object msg;
            int saveReaderIndex;
            // 从 netty 框架，读取到的数据，放入 message 后，下面就是针对 message 的反序列化和解码过程
            try {
                // decode object.
                do {
                    saveReaderIndex = message.readerIndex();
                    try {
                        // 解码，这里面包括的反序列化
                        msg = codec.decode(channel, message); // 重要，通过具体编码实例 codec 完成编码
                    } catch (IOException e) {
                        buffer = org.apache.dubbo.remoting.buffer.ChannelBuffers.EMPTY_BUFFER;
                        throw e;
                    }
                    if (msg == Codec2.DecodeResult.NEED_MORE_INPUT) { // 如果解码结果是 Codec2.DecodeResult.NEED_MORE_INPUT 表示需要更多的数据
                        message.readerIndex(saveReaderIndex); // 很重要，设置 readerIndex 为，解码读取前的位置，为了下次再从头读取
                        break;
                    } else {
                        if (saveReaderIndex == message.readerIndex()) {
                            buffer = org.apache.dubbo.remoting.buffer.ChannelBuffers.EMPTY_BUFFER;
                            throw new IOException("Decode without read data.");
                        }
                        if (msg != null) { // 解码完成，这里 msg 已经是 Request 对象
                            Channels.fireMessageReceived(ctx, msg, event.getRemoteAddress());
                        }
                    }
                } while (message.readable());
            } finally {
                if (message.readable()) {
                    message.discardReadBytes();
                    buffer = message;
                } else {
                    buffer = org.apache.dubbo.remoting.buffer.ChannelBuffers.EMPTY_BUFFER;
                }
                NettyChannel.removeChannelIfDisconnected(ctx.getChannel());
            }
        }

        // 处理异常
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            ctx.sendUpstream(e); // 向下传递，一般在最后一个 handler 处理异常
        }
    }
}