package org.apache.pig.impl.logicalLayer;

import java.io.Serializable;

public class OperatorKey implements Serializable {
    private static final long serialVersionUID = 1L;
    
    public String scope;
    public long id;

    public OperatorKey() {
        this("", -1);
    }
    
    public OperatorKey(String scope, long id) {
        this.scope = scope;
        this.id = id;
    }
    
    @Override
    public String toString() {
        return scope + "-" + id;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof OperatorKey)) {
            return false;
        }
        else {
            OperatorKey otherKey = (OperatorKey) obj;
            return this.scope.equals(otherKey.scope) &&
                   this.id == otherKey.id;
        }
    }
    
    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
    
    public String getScope() {
        return scope;
    }
    
    public long getId() {
        return id;
    }
}