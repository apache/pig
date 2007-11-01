/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.data;


import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;

import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.StarSpec;
import org.apache.pig.impl.io.DataBagFileReader;
import org.apache.pig.impl.io.DataBagFileWriter;


public class BigDataBag extends DataBag {
	
File tempdir;
LinkedList<File> stores = new LinkedList<File>();
DataBagFileWriter writer = null;
	
boolean finishedAdds = false,wantSorting = false, doneSorting = false, sortInProgress = false, wroteUnsortedFile = false;
long trueCount = 0;

boolean eliminateDuplicates = false;
EvalSpec spec = null;
	
public static long MAX_MEMORY = Runtime.getRuntime().maxMemory();
/**
 * Sets the limit of remaining memory that will
 * cause us to switch to disk backed mode
 */
public static long FREE_MEMORY_TO_MAINTAIN = (long)(MAX_MEMORY*.25);
	
	
public BigDataBag(Datum.DataType elementType, File tempdir) throws IOException
{
	super(elementType);
	this.tempdir = tempdir;
}
	
private boolean isMemoryAvailable(long memLimit)
{
	long freeMemory = Runtime.getRuntime().freeMemory();
	long usedMemory = Runtime.getRuntime().totalMemory() - freeMemory;
	return MAX_MEMORY-usedMemory > memLimit;
}
		
private void writeContentToDisk() throws IOException
{
	if (writer==null){
		File store = File.createTempFile("bag",".dat",tempdir);
		stores.add(store);
		writer = new DataBagFileWriter(store);
	}
	if (wantSorting && !wroteUnsortedFile){
		if (eliminateDuplicates)
			super.distinct();
		else
			super.sort(spec);
	}else{
		wroteUnsortedFile = true;
	}
	writer.write(content());
	super.clear();
	if (wantSorting){
		writer.close();
		writer = null;
	}
		
}
	
@Override
public void add(Datum d)
{
	if (d==null)
		return;
	if (finishedAdds) {
		throw new RuntimeException("DataBag has been read from");
	}
	try{
		if (writer == null) {
			//Want to add in memory
			super.add(d);
			if (!isMemoryAvailable(FREE_MEMORY_TO_MAINTAIN) && trueCount > 10) {
				writeContentToDisk();
			}	
		}else{
			writer.write(d);
		}
		trueCount++;
	} catch(IOException e) {
		throw new RuntimeException(e.getMessage());
	}
}
	
	
/**
 * @deprecated Use size instead.
 */
@Override
public int cardinality() { return (int)size(); }

@Override
public long size()
{
	if (!wantSorting || !eliminateDuplicates || doneSorting)
		return trueCount;
		
	if (sortInProgress)
		throw new RuntimeException("Can't ask for cardinality in the middle of a sort");
		
	//Now ask for the content to set the count right
	Iterator<Datum> iter = content();
		
	if (sortInProgress){
		//Must go through the entire iterator to set the count right
		while(iter.hasNext())
			iter.next();
	}
		
	return trueCount;
}
	
private void createSortedRuns() throws IOException
{
	DataBagFileReader reader = new DataBagFileReader(stores.removeFirst());
	Iterator<Datum> iter = reader.content();
	while(iter.hasNext()){
		DataBag bag = new DataBag(bagOf());
		while( iter.hasNext() && isMemoryAvailable(FREE_MEMORY_TO_MAINTAIN/2)){
			bag.add(iter.next());
		}
		if(eliminateDuplicates){
			bag.distinct();
			trueCount = bag.size();
		}else
			bag.sort(spec);
		File f = File.createTempFile("bag", ".dat",tempdir);
		stores.add(f);
		DataBagFileWriter writer = new DataBagFileWriter(f);
		writer.write(bag.content());
		bag.clear();
		writer.close();
	}
	reader.clear();
}
	
private Iterator<Datum> doSorting() throws IOException{
	if (wroteUnsortedFile){
		createSortedRuns();
	}
	
	if (stores.size()==1){
		doneSorting = true;
		return new DataBagFileReader(stores.peek()).content();
	}
	
	sortInProgress = true;
	while (true){
		Iterator<Datum> iter = new FileMerger();
		
		if (stores.size() > 1){
			while(iter.hasNext())
				iter.next();
		}else{
			return iter;
		}
	}
}
	
@Override
public Iterator<Datum> content()
{
	if (sortInProgress)
		throw new RuntimeException("Cannot open another iterator: a sort is in progress");
		
	finishedAdds = true;
		
	//memory only case
	if (stores.isEmpty()){
		if (wantSorting && !doneSorting){
			if (eliminateDuplicates){
				super.distinct();
				trueCount = super.size();
			}
			else
				super.sort(spec);
			doneSorting = true;
		}
		return super.content(); 
	}
	
	//disk case
	try{
		//first flush all remaining contents to disk too, and close any open files
		if (!isEmpty()) writeContentToDisk();
		
		close();
		
		//Now if not already sorted, sort the contents and return the iterator on 
		//the merged file
		if (wantSorting && !doneSorting){
			return doSorting();
		}
		
		//the list stores should be of length 1 at this time
		//because sorting always leaves it so
		//else just return the iterator on the singelton store file
		return new DataBagFileReader(stores.peek()).content();
	}catch(IOException e){
		throw new RuntimeException(e.getMessage());
	}
		
}
	
@Override
public boolean isEmpty() 
{
	return trueCount == 0;
}
	
@Override
public void remove(Datum d)
{
	throw new RuntimeException("BigDataBag is append only");
}
	
public void close()
{
	if (writer != null){
		try {
			writer.close();
			writer = null;
		} catch (IOException e) {
			RuntimeException ne = new RuntimeException(e.getMessage());
			ne.setStackTrace(e.getStackTrace());
			throw ne;
		}
	}
}
	
@Override
public void clear() {
	close();
	while(!stores.isEmpty())
		stores.removeFirst().delete();

	finishedAdds = false;
	trueCount = 0;
	wantSorting = false; doneSorting = false; sortInProgress = false; wroteUnsortedFile = false;
	super.clear();
}
		
private class HeapEntry
{
	DataBagFileReader reader;
	Iterator<Datum> iter;
	Datum	datum;

public HeapEntry(DataBagFileReader reader, Iterator<Datum> iter, Datum datum)
{
	this.reader = reader;
	this.iter = iter;
	this.datum = datum;
	}
	
}
	
private class FileMerger implements Iterator<Datum>
{
	PriorityQueue<HeapEntry> heap;
	private final int FANIN_LIMIT = 25;
	DataBagFileWriter writer;
	HeapEntry nextEntry;
		
public FileMerger() throws IOException
{
	Comparator<HeapEntry> comp = new Comparator<HeapEntry>(){

		public int compare(HeapEntry he1, HeapEntry he2) {
			try {
				return spec.getComparator().compare(he1.datum, he2.datum);
			} catch (RuntimeException e) {
				String msg = "spec = " + spec.toString() + "\n he1 = ";
				msg +=	he1.datum.toString();
				msg += "\n hev2 = ";
				msg += he2.datum.toString();	  
		
				throw new RuntimeException(e.getMessage() + ", additional info: " + msg);
			}
		}
	};
			
	heap = new PriorityQueue<HeapEntry>(10,comp);
			
	for (int i=0; i < FANIN_LIMIT && !stores.isEmpty(); i++){
		DataBagFileReader reader = new DataBagFileReader(stores.removeFirst());
		Iterator<Datum> iter = reader.content();
		if (iter.hasNext()){
			heap.add(new HeapEntry(reader,iter,iter.next()));
		}else{
			reader.clear();
		}
	}
			
	File outputFile  = File.createTempFile("bag",".dat",tempdir);
	stores.add(outputFile);
	writer = new DataBagFileWriter(outputFile);
			
	getNextEntry();
	if (eliminateDuplicates)
		trueCount = 0;
}
		
private void getNextEntry() throws IOException
{
	if (heap.isEmpty()){
		nextEntry = null;
		writer.close();
		if (stores.size()==1){
			sortInProgress = false;
			doneSorting = true;
		}
		return;
	}else{
		nextEntry = heap.poll();
		Iterator<Datum> iter = nextEntry.iter;
		if(iter.hasNext()){
			heap.add(new HeapEntry(nextEntry.reader,iter,iter.next()));
		}else{
			nextEntry.reader.clear();
		}
	}
}
		
public boolean hasNext()
{
	return nextEntry != null;
}
		
public Datum next()
{
	HeapEntry prevEntry = nextEntry;
	try{
		writer.write(prevEntry.datum);
		if (eliminateDuplicates)
			trueCount++;
		do{
			getNextEntry();
		}while(nextEntry!=null && eliminateDuplicates && spec.getComparator().compare(prevEntry.datum, nextEntry.datum)==0);			
				
	}catch(IOException e){
		RuntimeException re = new RuntimeException(e.getMessage());
		re.setStackTrace(e.getStackTrace());
		throw re;
	}
	return prevEntry.datum;
}
		
public void remove()
{
	throw new RuntimeException("Read only cursor");
}
		
}
	
private void sort(EvalSpec spec, boolean eliminateDuplicates)
{
	if (wantSorting)
		throw new RuntimeException("Can't request sorting again");
	if (trueCount > 0){
		//This is as good as starting to read, since we want to allow
		//sort specifications only in the beginning or the end
		finishedAdds = true;
	}
			
	wantSorting = true;
	this.spec = spec;
	this.eliminateDuplicates = eliminateDuplicates;
}
	
@Override
public void sort()
{
	sort(new StarSpec(),false);
	mIsSorted = true;
}
	
@Override
public void sort(EvalSpec spec)
{
	sort(spec,false);
	mIsSorted = true;
}
	
@Override
public void arrange(EvalSpec spec)
{
	sort(spec,false);
	mIsSorted = true;
}
	
@Override
public void distinct()
{
	sort(null,true);
	mIsSorted = true;
}

@Override
protected void finalize() throws Throwable
{
	clear();
	super.finalize();
}

}
