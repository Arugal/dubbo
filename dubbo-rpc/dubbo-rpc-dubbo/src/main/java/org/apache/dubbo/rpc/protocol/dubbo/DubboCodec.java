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
package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.io.Bytes;
import org.apache.dubbo.common.io.UnsafeByteArrayInputStream;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.serialize.ObjectOutput;
import org.apache.dubbo.common.serialize.Serialization;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.codec.ExchangeCodec;
import org.apache.dubbo.remoting.transport.CodecSupport;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.io.IOException;
import java.io.InputStream;

import static org.apache.dubbo.rpc.protocol.dubbo.CallbackServiceCodec.encodeInvocationArgument;


/**
 *
 *
 *前2个字节：
 *  为协议魔数，固定值oxdabb
 *
 * 第三字节：
 *  第1比特（0/1）表示是请求消息，还是响应消息
 *  第2比特（0/1）表示是是否必须双向通信,即有请求，必有响应
 *  第3比特（0/1）表示是是否是，心跳消息
 *  第低5位比特，表示一个表示消息序列化的方式(1,是dubbo ,2,是hessian...)
 *
 * 第四字节:
 *  只在响应消息中用到，表示响应消息的状态，是成功，失败等
 *
 * 第5-12字节：
 *  8个字节，表示一个long型数字，是reqeustId
 *
 * 第13—16字节：
 *  4个字节，表示消息体的长度（字节数）
 *
 * 消息体，不固定长度
 *  是请求消息时，表示请求数据
 *  是响应消息时，表示方法调用返回结果。
 */

/**
 * Dubbo codec.
 */
public class DubboCodec extends ExchangeCodec implements Codec2 {

    // 协议名
    public static final String NAME = "dubbo";
    // 协议版本
    public static final String DUBBO_VERSION = Version.getProtocolVersion();
    // 响应 - 异常
    public static final byte RESPONSE_WITH_EXCEPTION = 0;
    // 响应 - 正常（有返回）
    public static final byte RESPONSE_VALUE = 1;
    // 响应 - 正常 (空返回)
    public static final byte RESPONSE_NULL_VALUE = 2;
    //
    public static final byte RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS = 3;
    public static final byte RESPONSE_VALUE_WITH_ATTACHMENTS = 4;
    public static final byte RESPONSE_NULL_VALUE_WITH_ATTACHMENTS = 5;
    // 方法参数 - 空 (参数)
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    // 方法参数 - 空 (参数)
    public static final Class<?>[] EMPTY_CLASS_ARRAY = new Class<?>[0];
    private static final Logger log = LoggerFactory.getLogger(DubboCodec.class);

    /**
     * 解码,是从输出流 is 取字节数据，经反序列化，构造 Request 和 Response 对象的过程
     * @param channel
     * @param is
     * @param header
     * @return
     * @throws IOException
     */
    @Override
    protected Object decodeBody(Channel channel, InputStream is, byte[] header) throws IOException {
        // 消息头第3字节和 SERIALIZATION_MASK & 操作后，就可以得到 序列化/反序列化方案
        byte flag = header[2], proto = (byte) (flag & SERIALIZATION_MASK); // proto 获得 Serialization 对象
        // 根据序列化方案 id,或者 url 指定，通过 spi 机制去获取序列化实现，dubbo 协议默认用 hession2 序列化方案
        // 是放在消息头 flag 里的，这里 proto 值是 2
        // 获取具体用序列化/反序列化实现
        Serialization s = CodecSupport.getSerialization(channel.getUrl(), proto);
        // get request id.
        // id.header 字节从第5到12 8个字节，是请求 id
        long id = Bytes.bytes2long(header, 4);
        // 解析响应
        if ((flag & FLAG_REQUEST) == 0) { // 第一位是 0 -> 0 & 0 = 0/ 1 & 0 = 0/ 1 & 1 = 1 根据 flag & FLAG_REQUEST 后，判断需要解码的消息类型
            // decode response.
            Response res = new Response(id);
            if ((flag & FLAG_EVENT) != 0) {
                res.setEvent(Response.HEARTBEAT_EVENT);
            }
            // get status.
            // 获取响应状态 成功/失败等
            byte status = header[3];
            res.setStatus(status);
            if (status == Response.OK) { // 返回结果状态,OK 成功
                try {
                    Object data;
                    if (res.isHeartbeat()) {
                        data = decodeHeartbeatData(channel, deserialize(s, channel.getUrl(), is));
                    } else if (res.isEvent()) { // 事件消息，反序列化
                        data = decodeEventData(channel, deserialize(s, channel.getUrl(), is));
                    } else { // 业务调用结果消息，解码构造 DecodeableResult 对象的过程
                        DecodeableRpcResult result;
                        if (channel.getUrl().getParameter(
                                Constants.DECODE_IN_IO_THREAD_KEY,
                                Constants.DEFAULT_DECODE_IN_IO_THREAD)) { // 是否在 io 线程内解码 默认为 true
                            result = new DecodeableRpcResult(channel, res, is,
                                    (Invocation) getRequestData(id), proto);
                            // response 消息反序列化，就是把调用结果返回值，从 is 里反序列化出来，放在 DecodeableRpcResult 类的 result 字段的过程
                            result.decode();
                        } else {
                            // 不在 io 线程解码，要先通过 readMessageData 方法把调用结果数组取出后，放在 UnsafeByteArrayInputSteram 对象，
                            // 存在 DecodebleRpcResult 对象里，后续通过上层方法解码
                            result = new DecodeableRpcResult(channel, res,
                                    new UnsafeByteArrayInputStream(readMessageData(is)),
                                    (Invocation) getRequestData(id), proto);
                        }
                        data = result;
                    }
                    // 同时把 DecodeableRpcResult 对象放入 Response result 字段
                    res.setResult(data);
                } catch (Throwable t) {
                    if (log.isWarnEnabled()) {
                        log.warn("Decode response failed: " + t.getMessage(), t);
                    }
                    res.setStatus(Response.CLIENT_ERROR);
                    res.setErrorMessage(StringUtils.toString(t));
                }
            } else {
                // 异常处理，设置异常信息
                res.setErrorMessage(deserialize(s, channel.getUrl(), is).readUTF());
            }
            return res;
        } else { // 反解码 Request 消息类型
            // decode request.
            Request req = new Request(id);
            req.setVersion(Version.getProtocolVersion());
            req.setTwoWay((flag & FLAG_TWOWAY) != 0);
            if ((flag & FLAG_EVENT) != 0) { // 心跳事件 高字节，第三位不为 0
                req.setEvent(Request.HEARTBEAT_EVENT);
            }
            try {
                Object data;
                // 解码心跳事件
                if (req.isHeartbeat()) {
                    data = decodeHeartbeatData(channel, deserialize(s, channel.getUrl(), is));
                } else if (req.isEvent()) { // 解码其他事件
                    data = decodeEventData(channel, deserialize(s, channel.getUrl(), is));
                } else { // 解码其他响应
                    DecodeableRpcInvocation inv;
                    // 在通信框架（例如 Netty）的 IO 线程解码
                    // 业务调用请求消息，解码构造 DecodeableRpcInvocation 对象的过程
                    if (channel.getUrl().getParameter(
                            Constants.DECODE_IN_IO_THREAD_KEY,
                            Constants.DEFAULT_DECODE_IN_IO_THREAD)) { // 是否 io 线程内解码
                        inv = new DecodeableRpcInvocation(channel, req, is, proto);
                        // !!! Request 类型反序列化
                        inv.decode();
                    } else { // 在 Dubbo ThreadPool 线程解码，使用 DecodeHandler
                        inv = new DecodeableRpcInvocation(channel, req,
                                new UnsafeByteArrayInputStream(readMessageData(is)), proto);
                    }
                    data = inv;
                }
                // 同时把 DecodebleRpcInvocation 对象放入 Request data 字段
                req.setData(data);
            } catch (Throwable t) {
                if (log.isWarnEnabled()) {
                    log.warn("Decode request failed: " + t.getMessage(), t);
                }
                // bad request 异常请求对象设置
                req.setBroken(true);
                req.setData(t);
            }
            return req;
        }
    }

    /**
     * 获取反序列化方案
     * @param serialization
     * @param url
     * @param is
     * @return
     * @throws IOException
     */
    private ObjectInput deserialize(Serialization serialization, URL url, InputStream is)
            throws IOException {
        return serialization.deserialize(url, is);
    }


    /**
     * 读取 is 里的可用数据
     * @param is
     * @return
     * @throws IOException
     */
    private byte[] readMessageData(InputStream is) throws IOException {
        if (is.available() > 0) {
            byte[] result = new byte[is.available()];
            is.read(result);
            return result;
        }
        return new byte[]{};
    }

    @Override
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(channel, out, data, DUBBO_VERSION);
    }

    @Override
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeResponseData(channel, out, data, DUBBO_VERSION);
    }

    @Override
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        RpcInvocation inv = (RpcInvocation) data;

        // 写入 dubbo、path、version
        out.writeUTF(version);
        out.writeUTF(inv.getAttachment(Constants.PATH_KEY));
        out.writeUTF(inv.getAttachment(Constants.VERSION_KEY));

        // 写入方法，方法签名、方法参数集合
        out.writeUTF(inv.getMethodName());
        out.writeUTF(ReflectUtils.getDesc(inv.getParameterTypes()));
        Object[] args = inv.getArguments();
        if (args != null)
            for (int i = 0; i < args.length; i++) {
                out.writeObject(encodeInvocationArgument(channel, inv, i));
            }
        // 写入隐式传参集合
        out.writeObject(RpcUtils.getNecessaryAttachments(inv));
    }

    @Override
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        Result result = (Result) data;
        // currently, the version value in Response records the version of Request
        boolean attach = Version.isSupportResponseAttatchment(version);
        Throwable th = result.getException();
        // 正常
        if (th == null) {
            Object ret = result.getValue();
            // 空返回
            if (ret == null) {
                out.writeByte(attach ? RESPONSE_NULL_VALUE_WITH_ATTACHMENTS : RESPONSE_NULL_VALUE);
                // 有返回
            } else {
                out.writeByte(attach ? RESPONSE_VALUE_WITH_ATTACHMENTS : RESPONSE_VALUE);
                out.writeObject(ret);
            }
            //异常
        } else {
            out.writeByte(attach ? RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS : RESPONSE_WITH_EXCEPTION);
            out.writeObject(th);
        }

        if (attach) {
            // returns current version of Response to consumer side.
            result.getAttachments().put(Constants.DUBBO_VERSION_KEY, Version.getProtocolVersion());
            out.writeObject(result.getAttachments());
        }
    }
}
