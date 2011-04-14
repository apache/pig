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
package org.apache.pig.penny.impl.comm;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;

import org.apache.pig.data.DataReaderWriter;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.penny.Location;
import org.apache.pig.penny.LogicalLocation;
import org.apache.pig.penny.PhysicalLocation;



public class Message implements Serializable {
    
    private static final long serialVersionUID = 5067552813057868704L;

    public enum Type { REG, DEREG, TAINT, APP };
    
    private final Type type;
    public static final int NO_ACK = -1; // Ack number that represents that no ack is expected
    private int ack = NO_ACK; // the number to identify the ack of this message
    private final Location source;
    private final Location destination;
    private final Tuple body;
    
    public Message(Type type, int ack, Location source, Location destination, Tuple body) {
        this.type = type;
        this.source = source;
        this.destination = destination;
        this.body = body;
        this.ack = ack;
    }
    
    public Message(Type type, Location source, Location destination, Tuple body) {
        this.type = type;
        this.source = source;
        this.destination = destination;
        this.body = body;
    }
    
    public Message(int ack, Location source, Location destination, Tuple body) {
        this.type = Type.APP;
        this.source = source;
        this.destination = destination;
        this.body = body;
        this.ack = ack;
    }
    
    public Message(Location source, Location destination, Tuple body) {
        this.type = Type.APP;
        this.source = source;
        this.destination = destination;
        this.body = body;
    }
    
    public Message copy() {
        return new Message(type, ack, source, destination, body);
    }
    
    public Type type() {
        return type;
    }

    private static int nextAck = 1;
    synchronized private static int genUniqueAck() {
        return nextAck++;
    }
    
    public void genAck() {
        this.ack = genUniqueAck();
    }
    
    public int getAck() {
        return ack;
    }
    
    public Location source() {
        return source;
    }
    
    public Location destination() {
        return destination;
    }
    
    public Tuple body() {
        return body;
    }
    
    public Tuple toTuple() {
        Tuple t = new DefaultTuple();
        t.append(body);
        t.append(typeToString(type));
        t.append(ack);
        t.append(source.logId());
        t.append((source.isLogicalOnly())? "" : ((PhysicalLocation) source).physId());
        t.append(destination.logId());
        t.append((destination.isLogicalOnly())? "" : ((PhysicalLocation) destination).physId());
        return t;
    }
    
    public static Message fromTuple(Tuple t) throws IOException {
        Tuple body = (Tuple) t.get(0);
        Type type = stringToType((String) t.get(1));
        int ack = (Integer)t.get(2);
        Location source = (t.get(4) instanceof Integer)? new PhysicalLocation((String) t.get(3), (Integer) t.get(4)) : new LogicalLocation((String) t.get(3));
        Location destination = (t.get(6) instanceof Integer)? new PhysicalLocation((String) t.get(5), (Integer) t.get(6)) : new LogicalLocation((String) t.get(5));
        return new Message(type, ack, source, destination, body);
    }
    
    public void write(DataOutputStream out) throws IOException {
        DataReaderWriter.writeDatum(out, this.toTuple());
    }
    
    public static Message read(DataInputStream in) throws IOException {
        return fromTuple((Tuple) DataReaderWriter.readDatum(in));
    }
    
    private static String typeToString(Type type) {
        if (type == Type.REG) return "reg";
        else if (type == Type.DEREG) return "dereg";
        else if (type == Type.TAINT) return "taint";
        else if (type == Type.APP) return "app";
        else throw new RuntimeException("Unrecognized message type.");
    }
    
    private static Type stringToType(String s) {
        if (s.equals("reg")) return Type.REG;
        else if (s.equals("dereg")) return Type.DEREG;
        else if (s.equals("taint")) return Type.TAINT;
        else if (s.equals("app")) return Type.APP;
        else throw new RuntimeException("Unrecognized message type.");        
    }
    
    @Override
    public String toString() {
        return "Message(ack " + ack + ", type " + type + ", from " + source + ", to " + destination + ", body: " + body + ")";
    }

}
