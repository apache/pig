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
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.penny.PhysicalLocation;
import org.apache.pig.penny.impl.comm.Message;
import org.apache.pig.penny.impl.comm.Message.Type;
import org.apache.pig.penny.impl.comm.MessageReceiptCallback;
import org.apache.pig.penny.impl.comm.SyncCallResult;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

public class MessagingClient extends SimpleChannelHandler {
    ClientBootstrap bootstrap;
    Channel channel;
    MessageReceiptCallback cb;
    
    
    public class MessagingClientHandler extends SimpleChannelHandler {
        private HashMap<Integer, SyncCallResult> outstandingAcks = new HashMap<Integer, SyncCallResult>();
        
        public void addOutstandingAck(int ack, SyncCallResult scr) {
            outstandingAcks.put(ack, scr);
        }
        
         @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
                throws Exception {
            final Message message = (Message) e.getMessage();
            if (message.getAck() == Message.NO_ACK) {
                cb.receive(message);
            } else {
                SyncCallResult scr = outstandingAcks.remove(message.getAck());
                if (scr != null) {
                    synchronized(scr) {
                        scr.setTuple(message.body());
                        scr.notifyAll();
                    }
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
                throws Exception {
            cb.ioError((PhysicalLocation) ctx.getAttachment(), (Exception)e.getCause());
            super.exceptionCaught(ctx, e);
        }
        
    }
    
    final MessagingClientHandler messagingClientHandler = new MessagingClientHandler();
    InetSocketAddress masterAddr;
    MessagingClient(InetSocketAddress masterAddr, MessageReceiptCallback cb) {
        this.cb = cb;
        this.masterAddr = masterAddr;
        bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
        bootstrap.setPipeline(Channels.pipeline(new ObjectDecoder(MessagingServer.getMaxObjectSize()), new ObjectEncoder(), messagingClientHandler));
    }
    
    public void connect() throws Exception {
        ChannelFuture future = bootstrap.connect(masterAddr);
        future.await();
        if (!future.isSuccess()) {
            throw (Exception)future.getCause();
        }
        channel = future.getChannel();
    }
    
    
    public ChannelFuture sendAsync(Message o) {
        if (channel.isConnected()) {
            return channel.write(o);
        }
        return null;
    }
    
    public Tuple sendSync(Message m) throws IOException {
        m.genAck();
        SyncCallResult scr = new SyncCallResult();
        messagingClientHandler.addOutstandingAck(m.getAck(), scr);
        synchronized(scr) {
            sendAsync(m);
            while(scr.getException() == null && scr.getTuple() == null) {
                try {
                    scr.wait();
                } catch(InterruptedException e) {
                    throw new InterruptedIOException();
                }
            }
        }
        if (scr.getException() != null) {
            throw new IOException("Problem sending message", scr.getException());
        }
        return scr.getTuple();
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

    public void shutdown() {
        if (channel != null) {
            try {
                try {
                    channel.getCloseFuture().await(20000);
                } catch (InterruptedException e) {
                    // ignore, but regen just in case
                    Thread.currentThread().interrupt();
                }    
                if (channel.isConnected()) {
                    throw new RuntimeException("Not all messages sent")    ;
                }
            } finally {    
                if (channel.isOpen()) {
                    channel.close();
                }
                bootstrap.releaseExternalResources();
            }
        }
    }
    
}
