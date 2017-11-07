package com.nettyrpc.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

/**
 * RPC Decoder
 * @author huangyong
 */
public class RpcDecoder extends ByteToMessageDecoder {

    private Class<?> genericClass;

    public RpcDecoder(Class<?> genericClass) {
        this.genericClass = genericClass;
    }

    @Override
    public final void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 4) {
            return;
        }
        // 标记ByteBuf的readerIndex()的位置
        in.markReaderIndex();
        int dataLength = in.readInt();

        /**
         * readableBytes 返回可被读取的字节数
         * writeableBytes 返回可被写入的字节数
         */
        if (in.readableBytes() < dataLength) {
            // 重置ByteBuf的readerIndex()的位置
            in.resetReaderIndex();
            return;
        }
        byte[] data = new byte[dataLength];
        // 将ByteBuf中的数据读到data字节数组当做
        in.readBytes(data);

        // 对该字节数组进行反序列化
        Object obj = SerializationUtil.deserialize(data, genericClass);
        //Object obj = JsonUtil.deserialize(data,genericClass); // Not use this, have some bugs
        out.add(obj);
    }

}
