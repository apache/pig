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

package org.apache.pig.penny.test;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.penny.Communicator;
import org.apache.pig.penny.Coordinator;
import org.apache.pig.penny.Location;
import org.apache.pig.penny.LogicalLocation;
import org.apache.pig.penny.MonitorAgent;
import org.apache.pig.penny.NoSuchLocationException;
import org.apache.pig.penny.PhysicalLocation;
import org.apache.pig.penny.impl.comm.Message;
import org.apache.pig.penny.impl.harnesses.CoordinatorHarness;
import org.apache.pig.penny.impl.harnesses.MonitorAgentHarness;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Basic tests of the communication layer between agents and coordinator
 */
public class ComTest {
    static class SimpleCoordinator extends Coordinator {
        Serializable args[];
        boolean finished = false;
        List<Message> messages = new LinkedList<Message>();
        Exception lastException;
        
        @Override
        public void init(Serializable[] args) {
            
        }

        @Override
        public void receiveMessage(Location source, Tuple tuple) {
            final Message message = new Message(source, PhysicalLocation.coordinatorLocation(), tuple);
            //System.out.println("Got: " + message);
            synchronized(messages) {
                messages.add(message);
            }
            try {
                communicator().sendToAgents(source.asLogical(), tuple);
            } catch (NoSuchLocationException e) {
                lastException = e;
            }
        }

        @Override
        public Object finish() {
            finished = true;
            return "finished";
        }
    }
    
    static class SimpleMonitorAgent extends MonitorAgent {
        List<Location> lastLocations = new LinkedList<Location>();
        List<Tuple> lastTuples = new LinkedList<Tuple>();
        boolean finished;
        @Override
        public Set<Integer> furnishFieldsToMonitor() {
            return null;
        }

        @Override
        public void init(Serializable[] args) {
        }

        @Override
        public Set<String> observeTuple(Tuple t, Set<String> tags)
                throws ExecException {
            return null;
        }

        @Override
        public void receiveMessage(Location source, Tuple message) {
            synchronized(this) {
                lastTuples.add(message);
                lastLocations.add(source);
                this.notifyAll();
            }
        }

        public Communicator communicator() {
            return super.communicator();
        }
        @Override
        public void finish() {
            finished = true;
        }
    }
    
    CoordinatorHarness ch;
    MonitorAgentHarness mah1, mah2, mah3;

    static final Set<String> l1 = Collections.singleton("L1");
    static final Set<String> l2 = Collections.singleton("L2");
    static final Set<String> l3 = Collections.singleton("L3");
    static final Set<String> l1l2 = new HashSet<String>();
    static {
        l1l2.add("L1");
        l1l2.add("L2");
    }
    static final List<String> logicalIds = Lists.newArrayList("L1", "L2", "L3");
    static final Set<String> empty = new HashSet<String>();
    
    SimpleCoordinator sc;

    public void startup() throws Exception {
        sc = new SimpleCoordinator();
        ch = new CoordinatorHarness(sc);
        mah1 = new MonitorAgentHarness(new SimpleMonitorAgent(), new LogicalLocation("L1"), new InetSocketAddress("127.0.0.1", CoordinatorHarness.MASTER_LISTEN_PORT),
                logicalIds, empty, l2, l3, null, null);
        mah1.initialize();
        mah2 = new MonitorAgentHarness(new SimpleMonitorAgent(), new LogicalLocation("L2"), new InetSocketAddress("127.0.0.1", CoordinatorHarness.MASTER_LISTEN_PORT),
                logicalIds, l1, empty, l3, null, null);
        mah2.initialize();
        mah3 = new MonitorAgentHarness(new SimpleMonitorAgent(), new LogicalLocation("L3"), new InetSocketAddress("127.0.0.1", CoordinatorHarness.MASTER_LISTEN_PORT),
                logicalIds, empty, empty, l1l2, null, null);
        mah3.initialize();
        Assert.assertEquals(3, ch.getMessagingServer().getPlMap().size());
    }

    public void shutdown() {
        if (mah1 != null) {
            Assert.assertEquals(3, ch.getMessagingServer().getPlMap().size());
            mah1.finish();
        }
        mah2.finish();
        mah3.finish();
        while (ch.getMessagingServer().getPlMap().size() > 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        Assert.assertEquals(0, ch.getMessagingServer().getPlMap().size());
        ch.finish();
    }

    /**
     * Test the basic startup and shutdown of agents and coordinator. It tests the physical location
     * assignment logic and initial communication protocol.
     */
    @Test
    public void simpleTest() throws Exception {
        startup();
        shutdown();
    }
    
    /**
     * Test sending to the coordinator.
     */
    @Test
    public void testToCoordinator() throws Exception {
        startup();
        Tuple tuple1 = TupleFactory.getInstance().newTuple();
        tuple1.append(1);
        final SimpleMonitorAgent sma = (SimpleMonitorAgent)mah1.getMonitorAgent();
        sma.communicator().sendToCoordinator(tuple1);
        Tuple rsp;
        synchronized(sma) {
            while (sma.lastTuples.isEmpty()) {
                sma.wait(5000);
            }
            rsp = sma.lastTuples.get(0);
        }
        Assert.assertEquals(tuple1, rsp);
        Assert.assertEquals(1, ((SimpleCoordinator)ch.coordinator()).messages.size());
        shutdown();
    }    
    /**
     * Test closing while sending pending to the coordinator
     */
    @Test
    public void testPendingSends() throws Exception {
        startup();
        try {
            Tuple tuple1 = TupleFactory.getInstance().newTuple();
            tuple1.append(1);
            final SimpleMonitorAgent sma = (SimpleMonitorAgent)mah1.getMonitorAgent();
            final int count = 10000;
            for(int i = 0; i < count; i++) {
                sma.communicator().sendToCoordinator(tuple1);
            }
            mah1.finish();
            mah1 = null;
            int loops = 15;
            synchronized(sc.messages) {
                while (sc.messages.size() < count && loops-- > 0) {
                    sc.messages.wait(1000);
                }
            }
            Assert.assertEquals(count, sc.messages.size());
        } finally {
            try {
                shutdown();
            } catch(Exception e) {
            }
        }
    }
    
    /**
     * Tests sending messages to agents. In particular it makes sure messages sent to a logical location
     * will get to an agent even if it hasn't come up yet.
     */
    @Test
    public void testToAgent() throws Exception {
        startup();
        Tuple tuple1 = TupleFactory.getInstance().newTuple();
        tuple1.append(1);
        Tuple tuple2 = TupleFactory.getInstance().newTuple();
        tuple2.append(2);
        LogicalLocation ll2 = new LogicalLocation("L2");
        final SimpleMonitorAgent sma1 = (SimpleMonitorAgent)mah1.getMonitorAgent();
        sma1.communicator().sendToAgents(ll2, tuple1);
        sma1.communicator().sendToAgents(ll2, tuple2);
        int count = 10;
        Thread.sleep(10);
        while(((SimpleMonitorAgent)mah2.getMonitorAgent()).lastTuples.size() != 2 && count > 0) {
            Thread.sleep(400);
            count--;
        }
        List<Tuple> lastTuples = ((SimpleMonitorAgent)mah2.getMonitorAgent()).lastTuples;
        Assert.assertEquals(2, lastTuples.size());
        Assert.assertEquals(tuple1, lastTuples.remove(0));
        Assert.assertEquals(tuple2, lastTuples.remove(0));
        // now startup a new agent to see if it gets the queued up tuples
        MonitorAgentHarness mah4 = new MonitorAgentHarness(new SimpleMonitorAgent(), new LogicalLocation("L2"), new InetSocketAddress("127.0.0.1", CoordinatorHarness.MASTER_LISTEN_PORT),
                logicalIds, l1, empty, l3, null, null);
        mah4.initialize();
        Thread.sleep(10);
        while(((SimpleMonitorAgent)mah4.getMonitorAgent()).lastTuples.size() != 2 && count > 0) {
            Thread.sleep(400);
            count--;
        }
        lastTuples = ((SimpleMonitorAgent)mah4.getMonitorAgent()).lastTuples;
        Assert.assertEquals(2, lastTuples.size());
        Assert.assertEquals(tuple1, lastTuples.remove(0));
        Assert.assertEquals(tuple2, lastTuples.remove(0));    
        mah4.finish();
        shutdown();
    }
}
