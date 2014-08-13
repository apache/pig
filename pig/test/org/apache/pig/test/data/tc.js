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
importPackage(Packages.org.apache.pig.scripting.js)
// Pig UDFs 

generateRelationshipsForTC.outputSchema = "relationships:{t:(target:chararray, candidate:chararray)}"
function generateRelationshipsForTC(subject, object, status) {
	id1 = subject.id;
	bag = new Array({ target: id1, candidate: id1});
	if (object != null) {
		id2 = object.id;
		if (status == "MATCH" && id2 != id1) {
			bag.push({ target: id1, candidate: id2});
			bag.push({ target: id2, candidate: id1});
			bag.push({ target: id2, candidate: id2});
		}
	}
	return bag;
}

followRel.outputSchema = "rels:{t:(id1:chararray, id2:chararray, status:chararray)}"
function followRel(to_join, warshall) {
    outputBag = new Array();
    for each (w in warshall) {
        outputBag.push({ id1: w.id1, id2: w.id2, status: "followed" });        
        for each (t in to_join) {
            outputBag.push({ id1: t.id1, id2: w.id2, status: t.status });
        }
    }
    return outputBag;
}

coalesceLine.outputSchema="rels:{t:(id:chararray, status:chararray)}"
function coalesceLine(bag) {
    mergedGroup = new Object();
    for each (t in bag) {
        if (mergedGroup[t.id2] != "followed") {
            mergedGroup[t.id2] = t.status;
        }
    }
    outputBag = new Array();
    for (member in mergedGroup) {
        outputBag.push({ id: member, status: mergedGroup[member] });
    }
    return outputBag
}


function main() {
 
	input = "simple_table";
	format = "PigStorage";
	workDir = "tmp";
	output = workDir+"/output";
	 
	// INIT
	JSPig.fs("-rmr "+workDir+"/warshall_0")
	JSPig.fs("-rmr "+workDir+"/to_join_0")
	
	// generate relationship, reverse relationship and relationship to self (only for MATCH)
	// initialize to not followed
	result = JSPig.compile(
	"	input_data = LOAD '$input' USING "+format+" AS (target:tuple(id:chararray,attributes:map[]),candidate:tuple(id:chararray,attributes:map[]),status:chararray);\n"+
	"	warshall_0 = FOREACH input_data GENERATE FLATTEN(generateRelationshipsForTC(*)),'notfollowed';\n"+
	"	to_join_0 = FILTER warshall_0 BY $0!=$1;\n"+
	"	STORE warshall_0 INTO '$workDir/warshall_0' USING PigStorage;\n"+
	"	STORE to_join_0 INTO '$workDir/to_join_0' USING PigStorage;\n"
	).bind().run();
	
	// ----- MAIN LOOP, we don't want to iterate more than 10 times -----
    for (i=0; i<10; ++i) {
        n = i+1;
        println("-------- ITER "+n);
        JSPig.fs("-rmr "+workDir+"/warshall_"+n);
        JSPig.fs("-rmr "+workDir+"/to_join_"+n);
        job = JSPig.compile(
        "    warshall_n_minus_1 = LOAD '$workDir/warshall_$i' USING PigStorage AS (id1:chararray, id2:chararray, status:chararray);\n"+
        "    to_join_n_minus_1 = LOAD '$workDir/to_join_$i' USING PigStorage AS (id1:chararray, id2:chararray, status:chararray);\n"+
        "    joined = COGROUP to_join_n_minus_1 BY id2, warshall_n_minus_1 BY id1;\n"+
        "    followed = FOREACH joined GENERATE FLATTEN(followRel(to_join_n_minus_1,warshall_n_minus_1));\n"+
        "    followed = FOREACH followed GENERATE $0 AS id1, $1 AS id2, $2 AS status;\n"+
        "    followed_byid = GROUP followed BY id1;\n"+
        "    warshall_n = FOREACH followed_byid GENERATE group, FLATTEN(coalesceLine(followed.(id2, status)));\n"+
        "    to_join_n = FILTER warshall_n BY $2 == 'notfollowed' AND $0!=$1;\n"+
        "    STORE warshall_n INTO '$workDir/warshall_$n' USING PigStorage;\n"+
        "    STORE to_join_n INTO '$workDir/to_join_$n' USING PigStorage;\n"
        ).bind().runSingle();
        if (job.result("to_join_n").getNumberRecords() == 0) {
            break;
        }
           
    } 
    // ----- FINAL STEP -----
        
    println("-------- Computed in "+n+" iterations");
        
    JSPig.fs("-rmr "+output);
    JSPig.compile(
    "    warshall_final = LOAD '$workDir/warshall_$n' USING PigStorage AS (id1:chararray, id2:chararray, status:chararray);\n"+
    "    materialized = GROUP warshall_final BY id1;\n"+
    "    members = FOREACH materialized GENERATE warshall_final.id2 as members;\n"+
    "    group_result = DISTINCT members;\n"+
    "    STORE group_result INTO '$output' USING "+format+";\n"
    ).bind().run();

}