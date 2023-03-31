package com.demo.netty.test;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.nio.charset.StandardCharsets;

public class MessageDecoder extends LengthFieldBasedFrameDecoder {
    public MessageDecoder() {
        super(1024 * 1024, 0, 4, 0, 4);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null;
        }
        int length = frame.readInt();
        byte[] content = new byte[length];
        frame.readBytes(content);
        String message = new String(content, StandardCharsets.UTF_8);
        return message;
    }
}
