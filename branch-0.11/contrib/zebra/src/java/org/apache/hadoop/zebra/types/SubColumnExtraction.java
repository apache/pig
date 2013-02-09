/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.zebra.types;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashSet;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.schema.ColumnType;
import org.apache.hadoop.zebra.parser.ParseException;


/**
 * This class extracts a subfield from a column or subcolumn stored
 * in entirety on disk
 * It should be used only by readers whose serializers do not
 * support projection
 */
public class SubColumnExtraction {
	static class SubColumn {
		Schema physical;
		Projection projection;
		ArrayList<SplitColumn> exec = null;
		SplitColumn top = null; // directly associated with physical schema 
		SplitColumn leaf = null; // target tuple corresponding to projection

    // list of SplitColumns to be created maps on their children
    ArrayList<SplitColumn> sclist = new ArrayList<SplitColumn>();

		SubColumn(Schema physical, Projection projection) throws ParseException, ExecException
		{
			this.physical = physical;
			this.projection = projection;
			top = new SplitColumn(Partition.SplitType.RECORD);
			exec = new ArrayList<SplitColumn>();
			exec.add(top); // breadth-first 
		
			SplitColumn sc;
			leaf = new SplitColumn(Partition.SplitType.RECORD);
			Schema.ColumnSchema fs;
			Schema.ParsedName pn = new Schema.ParsedName();
			String name;
			int j;
			HashSet<String> keySet;
			for (int i = 0; i < projection.getSchema().getNumColumns(); i++)
			{
				fs = projection.getColumnSchema(i);
				if (fs == null)
				  continue;
				name = fs.getName();
				if (name == null)
					continue;
				if (projection.getKeys() != null)
				  keySet = projection.getKeys().get(fs);
				else
				  keySet = null;
				pn.setName(name);
				fs = physical.getColumnSchema(pn);
				if (keySet != null)
				  pn.setDT(ColumnType.MAP);
				if (fs == null)
		     		continue; // skip non-existing field
		
				j = fs.getIndex();
				ColumnType ct = pn.getDT();
				if (ct == ColumnType.MAP || ct == ColumnType.RECORD || ct == ColumnType.COLLECTION)
				{
					// record/map subfield is expected
					sc = new SplitColumn(j, ct);
          if (ct == ColumnType.MAP)
            sclist.add(sc);
					exec.add(sc); // breadth-first
					// (i, j) represents the mapping between projection and physical schema
					buildSplit(sc, fs, pn, i, (projection.getKeys() == null ? null :
                keySet));
				} else {
					// (i, j) represents the mapping between projection and physical schema
					sc = new SplitColumn(j, i, leaf, null, Partition.SplitType.NONE);
					// no split on a leaf
				}
				top.addChild(sc);
		 	}
		}

    /**
     * build the split executions
     */
		private void buildSplit(SplitColumn parent, Schema.ColumnSchema fs,
        final Schema.ParsedName pn, final int projIndex, HashSet<String> keys) throws ParseException, ExecException
		{
			// recursive call to get the next level schema
		  ColumnType ct = pn.getDT();
			if (ct != fs.getType())
	      	throw new ParseException(fs.getName()+" is not of proper type.");
	
			String prefix;
			int fieldIndex;
			SplitColumn sc;
			Partition.SplitType callerDT = (ct == ColumnType.MAP ? Partition.SplitType.MAP :
				                               (ct == ColumnType.RECORD ? Partition.SplitType.RECORD :
				                                 (ct == ColumnType.COLLECTION ? Partition.SplitType.COLLECTION :
				                        	         Partition.SplitType.NONE)));
			prefix = pn.parseName(fs);
			Schema schema = fs.getSchema();
			if (callerDT == Partition.SplitType.RECORD || callerDT == Partition.SplitType.COLLECTION)
			{
        if (keys != null)
          throw new AssertionError("Internal Logical Error: empty key map expected.");
				 if ((fieldIndex = schema.getColumnIndex(prefix)) == -1)
	        		return; // skip non-existing fields
				 fs = schema.getColumn(fieldIndex);
			} else {       
        parent.setKeys(keys); // map key is set at parent which is of type MAP
        fs = schema.getColumn(0); // MAP value is a singleton type!
				fieldIndex = 0;
			}
	
		  ct = pn.getDT();
			if (ct != ColumnType.ANY)
			{
				// record subfield is expected
			 	sc = new SplitColumn(fieldIndex, ct);
        if (ct == ColumnType.MAP)
          sclist.add(sc);
			 	exec.add(sc); // breadth-first
			 	buildSplit(sc, fs, pn, projIndex, null);
			} else {
				sc = new SplitColumn(fieldIndex, projIndex, leaf, null, Partition.SplitType.NONE);
				// no split on a leaf
			}
			parent.addChild(sc);
		}

    /**
     * dispatch the source tuple from disk
     */
		void dispatchSource(Tuple src)
		{
			top.dispatch(src);
	 	}

    /**
     * dispatch the target tuple
     */
		private void dispatch(Tuple tgt) throws ExecException
		{
			leaf.dispatch(tgt);
			createMaps();
			leaf.setBagFields();
		}

    /**
     * the execution
     */
		void splitColumns(Tuple dest) throws ExecException, IOException
		{
			int i;
			dispatch(dest);
            clearMaps();
            int execSize = exec.size();
			for (i = 0; i < execSize; i++)
			{
			    SplitColumn execElement = exec.get(i);
				if (execElement != null)
				{
					// split is necessary
					execElement.split();
				}
			}
		}

    /**
     * create MAP fields if necessary
     */
    private void createMaps() throws ExecException
    {
      for (int i = 0; i < sclist.size(); i++)
        sclist.get(i).createMap();
    }

    /**
     * clear map fields if necessary
     */
    private void clearMaps() throws ExecException
    {
      for (int i = 0; i < sclist.size(); i++)
        sclist.get(i).clearMap();
    }
	}

  /**
   * helper class to represent one execution
   */
  private static class SplitColumn {
	 int fieldIndex = -1; // field index to parent
	 int projIndex = -1; // index in projection: only used by leaves
	 ArrayList<SplitColumn> children = null;
	 int index = -1; // index in the logical schema 
	 Object field = null;
	 SplitColumn leaf = null; // leaf holds the target tuple
	 Partition.SplitType st = Partition.SplitType.NONE;
	 HashSet<String> keys;
	 Schema scratchSchema; // a temporary 1-column schema to be used to create a tuple
	                       // for a COLLETION column
	 ArrayList<Integer> bagFieldIndices;

	 void dispatch(Object field) { this.field = field; }

	 void setKeys(HashSet<String> keys) { this.keys = keys; }

	 SplitColumn(Partition.SplitType st)
	 {
		 this.st = st;
	 }

	 SplitColumn(ColumnType ct)
	 {
		 if (ct == ColumnType.MAP)
			 st = Partition.SplitType.MAP;
		 else if (ct == ColumnType.RECORD)
			 st = Partition.SplitType.RECORD;
		 else if (ct == ColumnType.COLLECTION)
		 {
		   st = Partition.SplitType.COLLECTION;
		   try {
		      scratchSchema = new Schema("foo");
		   } catch (ParseException e) {
		     // no-op: should not throw at all.
		   }
		 } else
			 st = Partition.SplitType.NONE;
	 }

	 SplitColumn(int fieldIndex, ColumnType ct)
	 {
		 this(ct);
		 this.fieldIndex = fieldIndex;
	 }

	 SplitColumn(int fieldIndex, Partition.SplitType st)
	 {
		 this.fieldIndex = fieldIndex;
		 this.st = st;
	 }

	 SplitColumn(int fieldIndex, HashSet<String> keys, Partition.SplitType st)
	 {
		 this.fieldIndex = fieldIndex;
		 this.keys = keys;
		 this.st = st;
	 }

	 SplitColumn(int fieldIndex,int projIndex, SplitColumn leaf, HashSet<String> keys, Partition.SplitType st)
	 {
		 this(fieldIndex, keys, st);
		 this.projIndex = projIndex;
		 this.leaf = leaf;
	 }
	 
   /**
    * the split op
    */
	 @SuppressWarnings("unchecked")
	 void split() throws IOException, ExecException
	 {
		 if (children == null)
			 return;
		 
		 int size = children.size();
		 if (st == Partition.SplitType.RECORD)
		 {
			 for (int i = 0; i < size; i++)
			 {
			     SplitColumn child = children.get(i);
				 if (child.projIndex != -1) // a leaf: set projection directly
			 		((Tuple)child.leaf.field).set(child.projIndex, ((Tuple) field).get(child.fieldIndex));
				 else
					 child.field = ((Tuple) field).get(child.fieldIndex);
			 }
		 } else if (st == Partition.SplitType.COLLECTION) {
		    DataBag srcBag, tgtBag;
		    srcBag = (DataBag) field;
		    Tuple tuple;
		    for (int i = 0; i < size; i++)
		    {
		      SplitColumn child = children.get(i);
		      if (child.projIndex != -1) // a leaf: set projection directly
		      {
		        tgtBag = (DataBag)((Tuple)child.leaf.field).get(child.projIndex);
		      } else {
		        tgtBag = (DataBag) child.field;
		        tgtBag.clear();
		      }
		      for (Iterator<Tuple> it = srcBag.iterator(); it.hasNext(); )
		      {
		        tuple = TypesUtils.createTuple(scratchSchema);
		        tuple.set(0, it.next().get(child.fieldIndex));
		        tgtBag.add(tuple);
		      }
		    }
		 } else if (st == Partition.SplitType.MAP && keys != null) {
       String key;
       Iterator<String> it;
       Object value;
			 for (int i = 0; i < size; i++)
			 {
			     SplitColumn child = children.get(i);
				 if (child.projIndex != -1) // a leaf: set projection directly
         {
           for (it = keys.iterator(); it.hasNext(); )
           {
             key = it.next();
             value = ((Map<String, Object>) field).get(key);
             if (value == null)
               continue;
			 		   ((Map<String, Object>) (((Tuple)child.leaf.field).get(child.projIndex))).put(key, value);
           }
         } else {
           for (it = keys.iterator(); it.hasNext(); )
           {
             key = it.next();
					   child.field = ((Map<String, Object>) field).get(key);
           }
         }
			 }
		 }
	 }

   /**
    * add a child that needs a subfield of this (sub)column
    */
	 void addChild(SplitColumn child) throws ExecException {
		 if (children == null)
			 children = new ArrayList<SplitColumn>();
		 children.add(child);
		 if (st == Partition.SplitType.COLLECTION) {
		   if (child.projIndex != -1)
		   {
		     child.leaf.addBagFieldIndex(child.projIndex);
		   } else {
		     ((Tuple) child.field).set(child.fieldIndex, TypesUtils.createBag());
		   }
		 }
	 }
	 
	 /**
	  * add a bag field index
	  */
	 void addBagFieldIndex(int i)
	 {
	   if (bagFieldIndices == null)
	     bagFieldIndices = new ArrayList<Integer>();
	   bagFieldIndices.add(i);
	 }
	 
	 /**
	  * set bag fields if necessary
	  */
	 void setBagFields() throws ExecException
	 {
	   if (bagFieldIndices == null)
	     return;
	   for (int i = 0; i < bagFieldIndices.size(); i++)
	   {
	     ((Tuple) field).set(bagFieldIndices.get(i), TypesUtils.createBag());
	   }
	 }

   /**
    * create MAP fields for children
    */
   void createMap() throws ExecException
   {
     if (st == Partition.SplitType.MAP)
     {
       int size = children.size();
       for (int i = 0; i < size; i++)
       {
				 if (children.get(i).projIndex != -1)
			 		 ((Tuple)children.get(i).leaf.field).set(children.get(i).projIndex, new HashMap<String, Object>());
				 else
           children.get(i).field = new HashMap<String, Object>();
       }
     }
   }
   
    /**
     * clear map for children
     */
	  @SuppressWarnings("unchecked")
    void clearMap() throws ExecException
    {
      if (st == Partition.SplitType.MAP)
      {
        int size = children.size();
        for (int i = 0; i < size; i++)
        {
	 			  if (children.get(i).projIndex != -1)
	 		 		  ((Map)((Tuple)children.get(i).leaf.field).get(children.get(i).projIndex)).clear();
          else
            ((Map)children.get(i).field).clear();
        }
      }
    }
   
  }
}
