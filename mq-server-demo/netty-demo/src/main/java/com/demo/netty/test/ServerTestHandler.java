package com.demo.netty.test;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.nio.charset.StandardCharsets;

public class ServerTestHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        String message = (String) msg;
        System.out.println("Received message: " + message);
        // 处理请求
        String response = "Hello, " + message;
        // 返回响应
        ByteBuf encoded = ctx.alloc().buffer();
//        encoded.writeInt(response.getBytes(StandardCharsets.UTF_8).length);
        encoded.writeBytes(response.getBytes(StandardCharsets.UTF_8));
        ctx.write(encoded);
        ctx.flush();
    }
}