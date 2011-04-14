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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.penny.Communicator;
import org.apache.pig.penny.Location;
import org.apache.pig.penny.LogicalLocation;
import org.apache.pig.penny.MonitorAgent;
import org.apache.pig.penny.NoSuchLocationException;
import org.apache.pig.penny.PhysicalLocation;
import org.apache.pig.penny.impl.comm.Message;
import org.apache.pig.penny.impl.comm.Message.Type;
import org.apache.pig.penny.impl.comm.MessageReceiptCallback;

public class MonitorAgentHarness {

    // message queue for each agent, for within-task messages
    private final static Map<String, List<Message>> withinTaskMessages = new HashMap<String, List<Message>>();
    
    private static void sendWithinTaskMessage(String dst, Message msg) {
        synchronized (withinTaskMessages) {
            if (!withinTaskMessages.containsKey(dst))
                withinTaskMessages.put(dst, new LinkedList<Message>());
            withinTaskMessages.get(dst).add(msg);
        }
    }

    private final MonitorAgent monitorAgent;

    /**
     * the set of valid logical ids
     */
    private final Set<String> logicalIds;

    /**
     * the logical ids upstream in the same task
     */
    private final Set<String> withinTaskUpstreamNeighbors;

    /**
     * the logical ids downstream in the same task
     */
    private final Set<String> withinTaskDownstreamNeighbors;

    /**
     * the logical ids downstream in outside the task
     */
    private final Set<String> crossTaskDownstreamNeighbors;

    private final LogicalLocation logicalLocation;

    private PhysicalLocation location;

    private MessagingClient senderReceiver;

    private Communicator communicator;

    private MessageReceiptCallback messageReceiptCallback;
    
    private InetSocketAddress masterAddr;

    // if the operator preceding this monitoring poitn is a cross-task operator
    // (group, join, etc.), this gives the group/join key -- otherwise this is null
    private final Map<String, List<Integer>> incomingCrossTaskKeyFields;

    // if the operator following this monitoring point is a cross-task operator
    // (group, join, etc.), this gives the group/join key -- otherwise this is null
    private final List<Integer> outgoingCrossTaskKeyFields;

    private Set<String> withinTaskDownstreamTaint = MonitorAgent.NO_TAGS;

    private Set<String> currentWithinTaskTaintTag = MonitorAgent.NO_TAGS;
    
    // sourceAlias -> (keyList -> tags)
    private final Map<String, Map<Tuple, Set<String>>> crossTaskTaintTags;
    
    /**
     * 
     * @param monitorAgent the agent object to invoke callbacks on.
     * @param logicalLocation the logical location of this agent.
     * @param masterAddr the address of the coordinator.
     * @param logicalIds the valid logical ids.
     * @param withinTaskUpstreamNeighbors the logical ids upstream in the same task.
     * @param withinTaskDownstreamNeighbors the logical ids downstream in the same task.
     * @param crossTaskDownstreamNeighbors the logical ids downstream outside the task
     * @param incomingCrossTaskKeyFields
     * @param outgoingCrossTaskKeyFields
     */
    public MonitorAgentHarness(MonitorAgent monitorAgent,
            LogicalLocation logicalLocation, InetSocketAddress masterAddr,
            List<String> logicalIds, Set<String> withinTaskUpstreamNeighbors,
            Set<String> withinTaskDownstreamNeighbors,
            Set<String> crossTaskDownstreamNeighbors,
            Map<String, List<Integer>> incomingCrossTaskKeyFields, List<Integer> outgoingCrossTaskKeyFields) {
        this.monitorAgent = monitorAgent;
        this.masterAddr = masterAddr;
        this.logicalLocation = logicalLocation;
        this.logicalIds = new HashSet<String>(logicalIds);
        this.withinTaskUpstreamNeighbors = new HashSet<String>(
                withinTaskUpstreamNeighbors);
        this.withinTaskDownstreamNeighbors = new HashSet<String>(
                withinTaskDownstreamNeighbors);
        this.crossTaskDownstreamNeighbors = new HashSet<String>(
                crossTaskDownstreamNeighbors);
        this.incomingCrossTaskKeyFields = incomingCrossTaskKeyFields;
        this.outgoingCrossTaskKeyFields = outgoingCrossTaskKeyFields;
        
        this.crossTaskTaintTags = new HashMap<String, Map<Tuple, Set<String>>>();
        if (incomingCrossTaskKeyFields != null) {
            for (String sourceAlias : incomingCrossTaskKeyFields.keySet()) {
                this.crossTaskTaintTags.put(sourceAlias, new HashMap<Tuple, Set<String>>());
            }
        }
    }

    public void initialize() throws Exception {
        this.communicator = new Communicator() {

            @Override
            public Location myLocation() {
                return location;
            }

            @Override
            public void sendToCoordinator(Tuple message) {
                senderReceiver.sendAsync(new Message(location, PhysicalLocation
                        .coordinatorLocation(), message));
            }

            @Override
            public void sendDownstream(Tuple message)
                    throws NoSuchLocationException {
                if (withinTaskDownstreamNeighbors.isEmpty()
                        && crossTaskDownstreamNeighbors.isEmpty())
                    throw new NoSuchLocationException("Line "
                            + logicalLocation.logId()
                            + " has no downstream neighbor.");
                for (String neighbor : withinTaskDownstreamNeighbors) {
                    sendWithinTaskMessage(neighbor, new Message(location,
                            new LogicalLocation(neighbor), message));
                }
                for (String neighbor : crossTaskDownstreamNeighbors) {
                    senderReceiver
                            .sendAsync(new Message(location,
                                    new LogicalLocation(neighbor), message));
                }
            }

            @Override
            public void sendUpstream(Tuple message)
                    throws NoSuchLocationException {
                if (withinTaskUpstreamNeighbors.isEmpty())
                    throw new NoSuchLocationException("Line "
                            + logicalLocation.logId()
                            + " has no within-task upstream neighbor.");
                for (String neighbor : withinTaskUpstreamNeighbors) {
                    sendWithinTaskMessage(neighbor, new Message(location,
                            new LogicalLocation(neighbor), message));
                }
            }

            @Override
            public void sendToAgents(LogicalLocation destination, Tuple tuple)
                    throws NoSuchLocationException {
                if (!logicalIds.contains(destination.logId()))
                    throw new NoSuchLocationException("Logical location "
                            + destination + " does not exist.");
                senderReceiver.sendAsync(new Message(location, destination, tuple));
            }

        };

        this.messageReceiptCallback = new MessageReceiptCallback() {
            public Tuple receive(Message m) {
                if (m.type() == Message.Type.TAINT) {
                    try {
                        handleTaintMessage(m);
                    } catch (ExecException e) {
                        throw new RuntimeException("Error in taint logic.", e);
                    }
                } else {
                    synchronized (monitorAgent) { // avoid concurrent calls to
                                                    // monitorAgent, and also
                                                    // wait for initialization
                                                    // to finish
                        monitorAgent.receiveMessage(m.source(), m.body());
                    }
                }
                return null; // default ack
            }

            @Override
            public void ioError(PhysicalLocation addr, Exception e) {
            }
        };

        synchronized (monitorAgent) { // block tuple-processing and message
                                        // receipt activity until initialization
                                        // finishes
            this.senderReceiver = new MessagingClient(masterAddr, messageReceiptCallback);
            this.senderReceiver.connect();
            
            monitorAgent.initialize(communicator);

            // send registration message to master
            Tuple regResponse = senderReceiver.sendSync(createRegistrationMessage());

            // first part of registration response contains assigned physical
            // location id
            this.location = new PhysicalLocation(logicalLocation,
                    (Integer) regResponse.get(0));

            // next part of registration response contains pending incoming
            // async messages
            for (Iterator<Tuple> it = ((DataBag) regResponse.get(1)).iterator(); it
                    .hasNext();) {
                messageReceiptCallback.receive(Message.fromTuple(it.next()));
            }
        }
    }

    public boolean processTuple(Tuple t) throws ExecException {
        processIncomingWithinTaskMessages(); // first, process any pending
                                                // incoming within-task messages

        Set<String> taintTags;
        synchronized (monitorAgent) { // avoid concurrent calls to
                                        // monitorAgent, and also wait for
                                        // initialization to finish
            taintTags = monitorAgent.observeTuple(t, getTaintTags(t));
        }
        if ((taintTags != MonitorAgent.FILTER_OUT) && (taintTags.size() > 0)) {
            if (outgoingCrossTaskKeyFields != null) {
                setCrossTaskDownstreamTaint(extractKeys(t, outgoingCrossTaskKeyFields), taintTags);
            } else {
                setWithinTaskDownstreamTaint(taintTags);
            }
        } else {
            clearWithinTaskDownstreamTaint();
        }
        return (taintTags != MonitorAgent.FILTER_OUT);
    }

    private void processIncomingWithinTaskMessages() {
        synchronized (withinTaskMessages) {
            List<Message> myQueue = withinTaskMessages.get(logicalLocation
                    .logId());
            while (myQueue != null && !myQueue.isEmpty()) {
                synchronized (monitorAgent) { // avoid concurrent calls to
                                                // monitorAgent, and also wait
                                                // for initialization to finish
                    messageReceiptCallback.receive(myQueue.remove(0));
                }
            }
        }
    }

    public void finish() {
        synchronized (monitorAgent) {
            monitorAgent.finish();
        }
        senderReceiver.sendAsync(new Message(Type.DEREG, location, PhysicalLocation.coordinatorLocation(), null));
        senderReceiver.shutdown();
    }

    private void clearWithinTaskDownstreamTaint() {
        setWithinTaskDownstreamTaint(MonitorAgent.NO_TAGS);
    }

    private void setCrossTaskDownstreamTaint(Tuple keys, Set<String> tags) {
        for (String neighbor : crossTaskDownstreamNeighbors) {
            Tuple body = new DefaultTuple();
            body.append("cross");
            body.append(keys);
            for (String tag : tags) {
                body.append(tag);
            }
            senderReceiver.sendAsync(new Message(Message.Type.TAINT, location,
                    new LogicalLocation(neighbor), body));
        }
    }

    private void setWithinTaskDownstreamTaint(Set<String> tags) {
        if (!withinTaskDownstreamTaint.equals(tags)) {
            Tuple body = new DefaultTuple();
            body.append("within");
            for (String tag : tags) {
                body.append(tag);
            }
            for (String neighbor : withinTaskDownstreamNeighbors) {
                sendWithinTaskMessage(neighbor, new Message(Message.Type.TAINT, location, new LogicalLocation(neighbor), body));
            }
            withinTaskDownstreamTaint = tags;
        }
    }

    private void handleTaintMessage(Message m) throws ExecException {
        if (m.body().get(0).equals("cross")) {         // cross-task (accumulates)
            String sourceAlias = m.source().logId();
            Map<Tuple, Set<String>> oneCTTT = crossTaskTaintTags.get(sourceAlias);
            
            Tuple keys = (Tuple) m.body().get(1);
            if (!oneCTTT.containsKey(keys)) {
                oneCTTT.put(keys, new HashSet<String>());
            }
            Set<String> tags = oneCTTT.get(keys);
            for (int i = 2; i < m.body().size(); i++) {
                tags.add((String) m.body().get(i));
            }
        } else {                                     // within-task (replaces)
            currentWithinTaskTaintTag = new HashSet<String>();
            for (int i = 1; i < m.body().size(); i++) {
                currentWithinTaskTaintTag.add((String) m.body().get(i));
            }
        }
    }

    private Set<String> getTaintTags(Tuple t) throws ExecException {
        if (incomingCrossTaskKeyFields == null) {     // within-task
            return currentWithinTaskTaintTag;
        } else {                                     // cross-task
            Set<String> tags = new HashSet<String>();
            for (String sourceAlias : incomingCrossTaskKeyFields.keySet()) {
                Tuple keys = extractKeys(t, incomingCrossTaskKeyFields.get(sourceAlias));
                Map<Tuple, Set<String>> oneCTTT = crossTaskTaintTags.get(sourceAlias);
                if (oneCTTT.containsKey(keys)) tags.addAll(oneCTTT.get(keys));
            }
            return tags;
        }
    }

    private static Tuple extractKeys(Tuple t, List<Integer> keyFields) throws ExecException {
        Tuple keys = new DefaultTuple();
        for (int keyField : keyFields) {
            keys.append(t.get(keyField));
        }
        return keys;
    }
    
    private Message createRegistrationMessage() {
        Tuple body = new DefaultTuple();
        return new Message(Message.Type.REG, logicalLocation, PhysicalLocation.coordinatorLocation(), body);
    }
    
    public MessagingClient getMessagingClient() {
        return senderReceiver;
    }
    
    public PhysicalLocation getLocation() {
        return location;
    }
    
    public MonitorAgent getMonitorAgent() {
        return monitorAgent;
    }
}
