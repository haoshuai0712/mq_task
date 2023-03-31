package com.demo.netty.test;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public class ClientTest {
    public static void main(String[] args) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .remoteAddress(new InetSocketAddress("localhost", 10088))
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new MessageEncoder());
                            pipeline.addLast(new MessageDecoder());
                            pipeline.addLast(new ClientTestHandler());
                        }
                    });

            ChannelFuture future = bootstrap.connect().sync();
            System.out.println("Connected to server: " + future.channel().remoteAddress());

            String message = "Netty test";
            ByteBuf encoded = future.channel().alloc().buffer();
            byte[] contentBytes = message.getBytes(StandardCharsets.UTF_8);
            System.out.println(contentBytes.length);
            encoded.writeInt(contentBytes.length);
            encoded.writeBytes(contentBytes);
            future.channel().write(encoded);
            future.channel().flush();
            System.out.println("Sent message: " + message);

            // 等待响应
            future.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully().sync();
        }
    }
}
