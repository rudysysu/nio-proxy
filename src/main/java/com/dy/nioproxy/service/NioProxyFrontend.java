package com.dy.nioproxy.service;

import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Service
public class NioProxyFrontend extends Thread {
    private Selector selector;
    private ServerSocketChannel ssc;

    private Map<SocketChannel, NioProxyBackend> cache = new HashMap<>();

    public NioProxyFrontend() throws IOException {
        selector = Selector.open();
        ssc = ServerSocketChannel.open();
        ssc.bind(new InetSocketAddress(8888));
        ssc.configureBlocking(false);
        ssc.register(selector, SelectionKey.OP_ACCEPT);

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
                    if (key.isAcceptable()) {
                        final SocketChannel sc = ssc.accept();
                        sc.configureBlocking(false);
                        sc.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                        cache.put(sc, new NioProxyBackend());
                    } else if (key.isReadable()) {
                        final SocketChannel channel = (SocketChannel) key.channel();
                        final NioProxyBackend nioProxyBackend = cache.get(channel);

                        ByteBuffer bb = ByteBuffer.allocate(1024);
                        final int read = channel.read(bb);
                        if (read > 0) {
                            bb.flip();
                            nioProxyBackend.outbox.addLast(bb);
                        }
                    } else if (key.isWritable()) {
                        final SocketChannel channel = (SocketChannel) key.channel();
                        final NioProxyBackend nioProxyBackend = cache.get(channel);

                        final ByteBuffer bb = nioProxyBackend.inbox.pollFirst();
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
