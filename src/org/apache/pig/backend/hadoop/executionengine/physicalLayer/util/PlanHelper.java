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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.util;

import java.util.List;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.*;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;

/**
 * Utility class with a few helper functions to deal with physical plans.
 */
public class PlanHelper {

    private final static Log log = LogFactory.getLog(new PlanHelper().getClass());
    
    private PlanHelper() {}

    /**
     * Get all the store operators in the plan in the right dependency order
     * @param plan
     * @return List of stores (could be empty)
     */
    public static List<POStore> getStores(PhysicalPlan plan) throws VisitorException {
        LoadStoreFinder finder = new LoadStoreFinder(plan);

        finder.visit();
        return finder.getStores();
    }

    /**
     * Get all the load operators in the plan in the right dependency order
     * @param plan
     * @return List of loads (could be empty)
     */
    public static List<POLoad> getLoads(PhysicalPlan plan) throws VisitorException {
        LoadStoreFinder finder = new LoadStoreFinder(plan);

        finder.visit();
        return finder.getLoads();
    }

    /**
     * Creates a relative path that can be used to build a temporary
     * place to store the output from a number of map-reduce tasks.
     */
    public static String makeStoreTmpPath(String orig) {
        Path path = new Path(orig);
        URI uri = path.toUri();
        uri.normalize();

        String pathStr = uri.getPath();
        if (path.isAbsolute()) {
            return new Path("abs"+pathStr).toString();
        } else {
            return new Path("rel/"+pathStr).toString();
        }
    }

    private static class LoadStoreFinder extends PhyPlanVisitor {
        private List<POLoad> loads;
        private List<POStore> stores;
        
        LoadStoreFinder(PhysicalPlan plan) {
            super(plan, new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan));
            stores = new LinkedList<POStore>();
            loads = new LinkedList<POLoad>();
        }
        
        @Override
        public void visit() throws VisitorException {
            super.visit();
        }
        
        @Override 
        public void visitStore(POStore st) throws VisitorException {
            super.visitStore(st);
            stores.add(st);
        }

        @Override 
        public void visitLoad(POLoad load) throws VisitorException {
            super.visitLoad(load);
            loads.add(load);
        }

        public List<POStore> getStores() {
            return stores;
        }

        public List<POLoad> getLoads() {
            return loads;
        }
    }
}
