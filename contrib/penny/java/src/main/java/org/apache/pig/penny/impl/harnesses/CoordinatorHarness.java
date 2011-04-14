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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.penny.Communicator;
import org.apache.pig.penny.Coordinator;
import org.apache.pig.penny.Location;
import org.apache.pig.penny.LogicalLocation;
import org.apache.pig.penny.NoSuchLocationException;
import org.apache.pig.penny.PhysicalLocation;
import org.apache.pig.penny.impl.comm.Message;
import org.apache.pig.penny.impl.comm.MessageReceiptCallback;


public class CoordinatorHarness implements MessageReceiptCallback {
    
    public final static int MASTER_LISTEN_PORT = 33335;

    private final Coordinator coordinator;
    
    // ADDED 10-19-10 BY CHRIS:
    private Communicator communicator;

    
    private final Map<LogicalLocation, Integer> maxPhysicalIds;                            // for each logical location, a counter for the physical ids that have been assigned so far
    private final Map<LogicalLocation, Collection<Message>> rendezvousMessages;            // messages sent to logical locations; need to send these to any future physical instances that register
    private final MessagingServer senderReceiver;                                        // utility for sending/receiving messages
    
    public MessagingServer getMessagingServer() {
        return senderReceiver;
    }
    
    public CoordinatorHarness(Coordinator coordinator) throws IOException {
        this.coordinator = coordinator;
        
        this.maxPhysicalIds = new HashMap<LogicalLocation, Integer>();
        this.rendezvousMessages = new HashMap<LogicalLocation, Collection<Message>>();
        
        this.senderReceiver = new MessagingServer(this, MASTER_LISTEN_PORT);
        this.senderReceiver.bind();
        
        // ADDED 10-19-10 BY CHRIS:
        this.communicator = new Communicator() {

            @Override
            public Location myLocation() {
                return PhysicalLocation.coordinatorLocation();
            }

            @Override
            public void sendToCoordinator(Tuple message) {
                throw new RuntimeException("This method not available from coordinator.");
            }

            @Override
            public void sendDownstream(Tuple message)
                    throws NoSuchLocationException {
                throw new RuntimeException("This method not available from coordinator.");
            }

            @Override
            public void sendUpstream(Tuple message)
                    throws NoSuchLocationException {
                throw new RuntimeException("This method not available from coordinator.");
            }

            @Override
            public void sendToAgents(LogicalLocation location, Tuple tuple)
                    throws NoSuchLocationException {
                handleSendRequest(false, new Message(PhysicalLocation.coordinatorLocation(), location, tuple));
            }

        };
        
        coordinator.initialize(communicator);

    }
    
    // ADDED 10-19-10 BY CHRIS:
    public Communicator communicator() { 
        return communicator; 
    }

    public Coordinator coordinator() {
        return coordinator;
    }
    
    @Override
    public Tuple receive(Message m) {
        return route(m);
    }

    public Object finish() {
        Object result;
        synchronized(coordinator) {
            result = coordinator.finish();
        }
        senderReceiver.shutdown();
        return result;
    }
    
    private Tuple route(Message m) {
        if (m.destination().equals(PhysicalLocation.coordinatorLocation())) {
            // message is for coordinator
            if (isRegistration(m)) {
                return processRegistration(m);
            } else if (isDeregistration(m)) {
                processDeregistration(m);
            } else {
                // regular message addressed to coordinator, so receive it locally
                synchronized(coordinator) {
                    coordinator.receiveMessage(m.source(), m.body());
                }
            }
        } else {
            // relay to slave(s)
            handleSendRequest((m.getAck() != Message.NO_ACK), m.copy());            // important to make a copy of the message, so original ack addr doesn't get overwritten
        }
        return null;        // default ack
    }
    
    private void initLLState(LogicalLocation ll) {
        maxPhysicalIds.put(ll, -1);
        rendezvousMessages.put(ll, new LinkedList<Message>());
    }
    
    private synchronized Tuple processRegistration(Message m) {
        // this method is synchronized to avoid concurrency problems between registering a new location and sending a message
        
        Location src = m.source();
        LogicalLocation logSrc = src.asLogical();
        
        if (!maxPhysicalIds.containsKey(logSrc)) {
            initLLState(logSrc);                    // first time we're seeing this logical location, so initialize some state for it
        }
        
        int physId = maxPhysicalIds.get(logSrc) + 1;
        maxPhysicalIds.put(logSrc, physId);
        PhysicalLocation physSrc = new PhysicalLocation(logSrc, physId);
    
        // create ack, which has two parts: (1) newly-assigned physical location id; (2) pending asynchronous messages
        Tuple ack = new DefaultTuple();
        ack.append(physSrc.physId());
        DataBag pendingMessages = BagFactory.getInstance().newDefaultBag();
        ack.append(pendingMessages);
        
        for (Message rm : rendezvousMessages.get(logSrc)) {
            pendingMessages.add(rm.toTuple());
            //send(false, rm, physSrc);        // this is a nonblocking (asynchronous) send request, so it's okay to have it inside a "synchronized" method
        }

        return ack;
    }
    
    private synchronized void processDeregistration(Message m) {
    }
    
    private InetSocketAddress extractListenAddr(Tuple body) {
        try {
            return new InetSocketAddress((String) body.get(0), (Integer) body.get(1));
        } catch (ExecException e) {
            throw new RuntimeException("Error extracting listen address from registration mesasge.", e);
        }
    }

    synchronized public void ioError(PhysicalLocation pl, Exception e) {
    }
    
    private synchronized void handleSendRequest(boolean synchronous, Message message) {
        // this method is synchronized to avoid concurrency problems between sending a message and registering a new location

        assert !synchronous: "We should not be sending synchronous requests from the coordinator";
        Location dst = message.destination();
        if (dst.isLogicalOnly()) {
            if (!maxPhysicalIds.containsKey(dst)) {
                initLLState(dst.asLogical());            // first time we're seeing this logical location, so initialize some state for it
            }
            
            // send to all currently registered physical instances of the logical destination
            for (int physId = 0; physId <= maxPhysicalIds.get(dst); physId++) {
                senderReceiver.sendAsync(message, new PhysicalLocation((LogicalLocation)dst, physId));
            }
            
            // put on rendezvous list -- to be sent to any additional physical instances that register in the future
            rendezvousMessages.get(dst).add(message);
        } else {
            // send to one physical location
            senderReceiver.sendAsync(message, (PhysicalLocation) dst);        // this is a nonblocking (asynchronous) send request, so it's okay to have it inside a "synchronized" method
        }
    }
    
    private boolean isRegistration(Message m) {
        return (m.type() == Message.Type.REG);
    }
    
    private boolean isDeregistration(Message m) {
        return (m.type() == Message.Type.DEREG);
    }
    
}
