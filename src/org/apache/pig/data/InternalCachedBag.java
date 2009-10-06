
package org.apache.pig.data;

import java.io.*;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;


public class InternalCachedBag extends DefaultAbstractBag {

	private static final Log log = LogFactory.getLog(InternalCachedBag.class);
    private int cacheLimit;
    private long maxMemUsage;
    private long memUsage;
    private DataOutputStream out;
    private boolean addDone;
    private TupleFactory factory;

 
    public InternalCachedBag() {
        this(1);
    }

    public InternalCachedBag(int bagCount) {       
        float percent = 0.5F;
        
    	if (PigMapReduce.sJobConf != null) {
    		String usage = PigMapReduce.sJobConf.get("pig.cachedbag.memusage");
    		if (usage != null) {
    			percent = Float.parseFloat(usage);
    		}
    	}

        init(bagCount, percent);
    }  
    
    public InternalCachedBag(int bagCount, float percent) {
    	init(bagCount, percent);
    }
    
    private void init(int bagCount, float percent) {
    	factory = TupleFactory.getInstance();        
    	mContents = new ArrayList<Tuple>();             
             	 
    	long max = Runtime.getRuntime().maxMemory();
        maxMemUsage = (long)(((float)max * percent) / (float)bagCount);
        cacheLimit = Integer.MAX_VALUE;
        
        // set limit to 0, if memusage is 0 or really really small.
        // then all tuples are put into disk
        if (maxMemUsage < 1) {
        	cacheLimit = 0;
        }
        
        addDone = false;
    }

    public void add(Tuple t) {
    	
        if(addDone) {
            throw new IllegalStateException("InternalCachedBag is closed for adding new tuples");
        }
                
        if(mContents.size() < cacheLimit)  {
            mMemSizeChanged = true;
            mContents.add(t);
            if(mContents.size() < 100)
            {
                memUsage += t.getMemorySize();
                long avgUsage = memUsage / (long)mContents.size();
                cacheLimit = (int)(maxMemUsage / avgUsage);
            }
        } else {
            try {
                if(out == null) {
                	if (log.isDebugEnabled()) {
                		log.debug("Memory can hold "+ mContents.size() + " records, put the rest in spill file.");
                	}
                    out = getSpillFile();
                }
                t.write(out);
            }
            catch(IOException e) {
                throw new RuntimeException(e);
            }
        }
        
        mSize++;
    }

    private void addDone() {
        if(out != null) {
            try {
                out.flush();
                out.close();
            }
            catch(IOException e) { 
            	// ignore
            }
        }
        addDone = true;
    }

    public void clear() {
    	if (!addDone) {
    		addDone();
    	}
        super.clear();
        addDone = false;
        out = null;
    }

    protected void finalize() {
    	if (!addDone) {
    		// close the spill file so it can be deleted
    		addDone();
    	}
    	super.finalize();
    }
    
    public boolean isDistinct() {
        return false;
    }

    public boolean isSorted() {
        return false;
    }

    public Iterator<Tuple> iterator() {
    	if(!addDone) {
    		// close the spill file and mark adding is done
    		// so further adding is disallowed.
    		addDone();
        }        
        return new CachedBagIterator();
    }

    public long spill()
    {
        throw new RuntimeException("InternalCachedBag.spill() should not be called");
    }
    
    private class CachedBagIterator implements Iterator<Tuple> {
        Iterator<Tuple> iter;
        DataInputStream in;
        Tuple next;
        
        public CachedBagIterator() {
            iter = mContents.iterator();
            if(mSpillFiles != null && mSpillFiles.size() > 0) {
                File file = (File)mSpillFiles.get(0);
                try {
                    in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
                }
                catch(FileNotFoundException fnfe) {
                    String msg = "Unable to find our spill file.";
                    throw new RuntimeException(msg, fnfe);
                }
            }
        }


        public boolean hasNext() {
        	if (next != null) {
        		return true;        		
        	}
        	
            if(iter.hasNext()){
                next = (Tuple)iter.next();
                return true;
            }
            
            if(in == null) {
                return false;
            }
            
            try {
            	Tuple t = factory.newTuple();
            	t.readFields(in);
            	next = t;
            	return true;
            }catch(EOFException eof) {
            	try{
            		in.close();
            	}catch(IOException e) {
            		
            	}            
            	in = null;
            	return false;
            }catch(IOException e) {            	 
                String msg = "Unable to read our spill file.";
                throw new RuntimeException(msg, e);               
            }
        }

        public Tuple next() {  
        	if (next == null) {
        		if (!hasNext()) {
        			throw new IllegalStateException("No more elements from iterator");
        		}
        	}
        	Tuple t = next;
        	next = null;
        	
        	return t;
        }

        public void remove() {
        	throw new UnsupportedOperationException("remove is not supported for CachedBagIterator");
        }

        protected void finalize() {
            if(in != null) {
                try
                {
                    in.close();
                }
                catch(Exception e) { 
                	
                }
            }
        }
    }

}

