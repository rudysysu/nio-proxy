package com.dy.nioproxy.service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;

public class NioProxyBackend extends Thread {
    private Selector selector;
    private SocketChannel sc;

    public LinkedBlockingDeque<ByteBuffer> outbox = new LinkedBlockingDeque<>(); // to mysql
    public LinkedBlockingDeque<ByteBuffer> inbox = new LinkedBlockingDeque<>(); // from mysql

    public NioProxyBackend() throws IOException {
        this.sc = SocketChannel.open();
        this.sc.configureBlocking(false);
        this.sc.connect(new InetSocketAddress("10.211.55.101", 3306));

        selector = Selector.open();
        this.sc.register(selector, SelectionKey.OP_CONNECT);

        this.start();
    }

    @Override
    public void run() {
        while (true) {
            try {
                selector.select();

                final Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    final SelectionKey key = iterator.next();

                    if (key.isConnectable()) {
                        SocketChannel channel = (SocketChannel) key.channel();
                        channel.finishConnect();
                        channel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                    } else if (key.isReadable()) {
                        SocketChannel channel = (SocketChannel) key.channel();
                        ByteBuffer bb = ByteBuffer.allocate(1024);
                        final int read = channel.read(bb);
                        if (read > 0) {
                            bb.flip();
                            inbox.addLast(bb);
                        }
                    } else if (key.isWritable()) {
                        SocketChannel channel = (SocketChannel) key.channel();
                        final ByteBuffer bb = outbox.pollFirst();
                        if (bb != null) {
                            channel.write(bb);
                        }
                    }
                }
                selector.selectedKeys().clear();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
