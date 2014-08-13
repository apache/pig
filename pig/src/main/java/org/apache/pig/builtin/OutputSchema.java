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
package org.apache.pig.builtin;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import org.apache.pig.EvalFunc;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * An EvalFunc can annotated with an <code>OutputSchema</code> to
 * tell Pig what the expected output is. This can be used in place
 * of {@link EvalFunc#outputSchema(Schema)}
 * <p>
 * The default implementation of {@link EvalFunc#outputSchema(Schema)}
 * will look at this annotation and return an interpreted schema, if the annotation is present.
 * <p>
 * Implementing a custom {@link EvalFunc#outputSchema(Schema)} will
 * override the annotation (unless you deal with it explicitly, or by calling <code>super.outputSchema(schema)</code>).
 * <p>
 * Here's an example of a complex schema declared in an annotation:
 * <code>@OutputSchema("y:bag{t:tuple(len:int,word:chararray)}")</code>
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
@Documented
@Retention(value=RetentionPolicy.RUNTIME)
public @interface OutputSchema {
    String value();
}
