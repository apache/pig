/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.penny.impl.harnesses;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.pig.data.Tuple;
import org.apache.pig.penny.PhysicalLocation;
import org.apache.pig.penny.impl.comm.Message;
import org.apache.pig.penny.impl.comm.Message.Type;
import org.apache.pig.penny.impl.comm.MessageReceiptCallback;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

public class MessagingServer {
    ServerBootstrap bootstrap;
    int port;
    ChannelGroup channelGroup;
    final MessagingServerHandler messagingServerHandler;

  private static final boolean DEBUG = !Boolean.getBoolean("Penny.server.disable_message_debug");
    
    public Map<PhysicalLocation, Channel> getPlMap() {
        return Collections.unmodifiableMap(messagingServerHandler.plMap);
    }
    
    static class MessagingServerHandler extends SimpleChannelHandler {
        // Maps the physical location to the channel to send to
        Map<PhysicalLocation, Channel> plMap = new ConcurrentHashMap<PhysicalLocation, Channel>();
        MessageReceiptCallback cb;
        
        public MessagingServerHandler(MessageReceiptCallback cb) {
            this.cb = cb;
        }
        
        @Override
        public void channelDisconnected(ChannelHandlerContext ctx,
                ChannelStateEvent e) throws Exception {
            PhysicalLocation pl = (PhysicalLocation)ctx.getAttachment();
            if (pl != null) {
                plMap.remove(pl);
            }
            super.channelDisconnected(ctx, e);
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
                throws Exception {
            final Channel channel = ctx.getChannel();
            Message m = (Message)e.getMessage();
            Tuple t = cb.receive(m);
            if (m.type() == Type.DEREG) {
                channel.close();
            }
            if (t != null) {
                if (m.type() == Type.REG) {
                    PhysicalLocation pl = new PhysicalLocation(m.source().logId(), (Integer)t.get(0));
                    plMap.put(pl, channel);
                    ctx.setAttachment(pl);
                }
                Message rsp = new Message(m.type(), m.getAck(), m.destination(), m.source(), t);
                channel.write(rsp);
            }
        }
        
        void sendMessage(Message m, PhysicalLocation pl) {
            Channel channel = plMap.get(pl);
            if (channel != null) {
                channel.write(m);
            }
        }
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
                throws Exception {
            if (e.getCause() instanceof ClosedChannelException) {
                // these are part of life
                // XXX Debug logging might be good
                return;
            }
            super.exceptionCaught(ctx, e);
        }
    }
    
    private static final int defaultMaxObjectSize = 64*1024*1024; 
    static int getMaxObjectSize() {
        String sizeString = System.getProperty("penny.maxObjectSize", Integer.toString(defaultMaxObjectSize));
        try {
            return Integer.parseInt(sizeString);
        } catch(Exception e) {
            return defaultMaxObjectSize;
        }
    }
    
    public MessagingServer(MessageReceiptCallback cb, int port) {
        this.messagingServerHandler = new MessagingServerHandler(cb);
        this.port = port;
        
        // Start server with Nb of active threads = 2*NB CPU + 1 as maximum.
        ChannelFactory factory = new NioServerSocketChannelFactory(Executors
                .newCachedThreadPool(), Executors.newCachedThreadPool(),
                Runtime.getRuntime().availableProcessors() * 2 + 1);
 
        bootstrap = new ServerBootstrap(factory);
        // Create the global ChannelGroup
        channelGroup = new DefaultChannelGroup(MessagingServer.class.getName());
        // 200 threads max, Memory limitation: 1MB by channel, 1GB global, 100 ms of timeout
        OrderedMemoryAwareThreadPoolExecutor pipelineExecutor = new OrderedMemoryAwareThreadPoolExecutor(
                200, 1048576, 1073741824, 100, TimeUnit.MILLISECONDS, Executors
                        .defaultThreadFactory());
 
        // We need to use a pipeline factory because we are using stateful handlers
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new ObjectDecoder(getMaxObjectSize()), new ObjectEncoder(), messagingServerHandler);
            }
        });
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("child.reuseAddress", true);
        bootstrap.setOption("child.connectTimeoutMillis", 100);
        bootstrap.setOption("readWriteFair", true);
    }
    
    public void bind() {
        Channel channel = bootstrap.bind(new InetSocketAddress(port));
        channelGroup.add(channel);
    }
    
    public void shutdown() {
        channelGroup.close().awaitUninterruptibly();
        bootstrap.releaseExternalResources();
    }
    
    public void sendAsync(Message m, PhysicalLocation l) {
        messagingServerHandler.sendMessage(m, l);
    }

    
}
