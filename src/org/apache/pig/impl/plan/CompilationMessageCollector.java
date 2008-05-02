package org.apache.pig.impl.plan ;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList ;
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
    
    public static class Message {
        private String msg = null ;
        private MessageType msgType = MessageType.Unknown ;
        
        public Message(String message, MessageType messageType) {
            msg = message ;
            msgType = messageType ;
        }
        
        public String getMessage() {
            return msg ;
        }
        
        public MessageType getMessageType() {
            return msgType ;
        }
    }
    
    private List<Message> messageList = new ArrayList<Message>() ;
    
    public CompilationMessageCollector() {
        // nothing here
    }
    
    public void collect(String message, MessageType messageType) {
        messageList.add(new Message(message, messageType)) ;
    }
    
    public boolean hasError() {
        Iterator<Message> iter = iterator() ;
        while(iter.hasNext()) {
            if (iter.next().getMessageType() == MessageType.Error) {
                return true ;
            }
        }
        return false ;
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
    
}
