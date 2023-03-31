package com.demo.netty.test;

import io.netty.handler.codec.LengthFieldPrepender;

public class MessageEncoder extends LengthFieldPrepender {
    public MessageEncoder() {
        super(4);
    }
}