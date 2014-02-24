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
package org.apache.pig.impl.plan ;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList ;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.pig.PigWarning;
/***
 * This class is used for collecting all messages (error + warning) in
 * compilation process. These messages are reported back to users
 * at the end of compilation.
 *
 * iterator() has to be called after CompilationMessageCollector is fully
 * populated otherwise the state is undefined.
 */
public class CompilationMessageCollector implements Iterable<CompilationMessageCollector.Message> {

    public enum MessageType {
        Unknown,
        Error,
        Warning,
        Info
    }

    public enum Unknown {
       UNKNOWN_MESSAGE_KIND;
       public String toString() {
         return "Aggregated unknown kind messages.  Please set -Daggregate.warning=false to retrieve these messages";
       }
    }

    public static class Message {
        private String msg = null ;
        private MessageType msgType = MessageType.Unknown ;
        private Enum kind = null;

        public Message(String message, MessageType messageType) {
            msg = message ;
            msgType = messageType ;
        }

        public Message(String message, MessageType messageType, Enum kind) {
            this(message, messageType);
            this.kind = kind;
        }

        public String getMessage() {
            return msg ;
        }

        public MessageType getMessageType() {
            return msgType ;
        }

        public Enum getKind() {
            return kind;
        }
    }

    private List<Message> messageList = new ArrayList<Message>() ;

    public CompilationMessageCollector() {
        // nothing here
    }

    public void collect(String message, MessageType messageType) {
        messageList.add(new Message(message, messageType,
                                    Unknown.UNKNOWN_MESSAGE_KIND)) ;
    }

    public void collect(String message, MessageType messageType, Enum kind) {
        messageList.add(new Message(message, messageType, kind)) ;
    }

    protected boolean hasMessageType(MessageType messageType) {
        Iterator<Message> iter = iterator() ;
        while(iter.hasNext()) {
            if (iter.next().getMessageType() == messageType) {
                return true ;
            }
        }
        return false ;
    }

    public boolean hasError() {
        return hasMessageType(MessageType.Error);
    }

    public Iterator<Message> iterator() {
        return messageList.iterator() ;
    }

    public boolean hasMessage() {
        return messageList.size() > 0 ;
    }

    public int size() {
        return messageList.size() ;
    }

    public Message get(int i) {
        return messageList.get(i) ;
    }

    public Map<Enum, Long> getKindAggregate(MessageType messageType) {
        Map<Enum, Long> aggMap = new HashMap<Enum, Long>();
        Iterator<Message> iter = iterator() ;
        while(iter.hasNext()) {
            Message message = iter.next();
            if (message.getMessageType() == messageType) {
                Enum kind = message.getKind();
                if(kind != null) {
                    Long count = aggMap.get(kind);
                    count = (count == null? 1 : ++count);
                    aggMap.put(kind, count);
              }
            }
        }
        return aggMap;
    }

    public static void logAggregate(Map<Enum, Long> aggMap, MessageType messageType, Log log) {
        long nullCounterCount = aggMap.get(PigWarning.NULL_COUNTER_COUNT)==null?0 : aggMap.get(PigWarning.NULL_COUNTER_COUNT);
        if (nullCounterCount!=0 && aggMap.size()>1) // PigWarning.NULL_COUNTER_COUNT is definitely in appMap
            logMessage("Unable to retrieve hadoop counter for " + nullCounterCount +
                    " jobs, the number following warnings may not be correct", messageType, log);
        for(Map.Entry<Enum, Long> e: aggMap.entrySet()) {
            if (e.getKey() !=PigWarning.NULL_COUNTER_COUNT)
            {
                Long count = e.getValue();
                if(count != null && count > 0) {
                    String message = "Encountered " + messageType + " " + e.getKey().toString() + " " + count + " time(s).";
                    logMessage(message, messageType, log);
                }
            }
        }
    }

    public static void logMessages(CompilationMessageCollector messageCollector,
            MessageType messageType, boolean aggregate, Log log) {
        if(aggregate) {
            Map<Enum, Long> aggMap = messageCollector.getKindAggregate(messageType);
            logAggregate(aggMap, messageType, log);
        } else {
            Iterator<Message> messageIter = messageCollector.iterator();
            while(messageIter.hasNext()) {
                Message message = messageIter.next();
                if(message.getMessageType() == messageType) {
                    logMessage(message.getMessage(), messageType, log);
                }
            }
        }
    }

    public void logMessages(MessageType messageType, boolean aggregate, Log log) {
        logMessages(this, messageType, aggregate, log);
    }

    public static void logAllMessages(CompilationMessageCollector messageCollector, Log log) {
        Iterator<Message> messageIter = messageCollector.iterator();
        while(messageIter.hasNext()) {
            Message message = messageIter.next();
            logMessage(message.getMessage(), message.getMessageType(), log);
        }
    }

    public void logAllMessages(Log log) {
        logAllMessages(this, log);
    }

    private static void logMessage(String messageString, MessageType messageType, Log log) {
        switch(messageType) {
        case Info:
            log.info(messageString);
            break;
        case Warning:
            log.warn(messageString);
            break;
        case Error:
            log.error(messageString);
        }
    }

}
