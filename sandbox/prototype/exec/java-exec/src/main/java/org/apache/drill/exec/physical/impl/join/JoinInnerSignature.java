/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.exec.compile.sig.CodeGeneratorSignature;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.VectorContainer;

import javax.inject.Named;

import static org.apache.drill.exec.compile.sig.GeneratorMapping.GM;


public interface JoinInnerSignature extends CodeGeneratorSignature {

  public static final MappingSet SETUP_MAPPING =
      new MappingSet("null", "null", 
                     GM("doSetup", "doSetup", null, null),
                     GM("doSetup", "doSetup", null, null));
  public static final MappingSet COPY_LEFT_MAPPING =
      new MappingSet("leftIndex", "outIndex",
                     GM("doSetup", "doCopyLeft", null, null),
                     GM("doSetup", "doCopyLeft", null, null));
  public static final MappingSet COPY_RIGHT_MAPPING =
      new MappingSet("rightIndex", "outIndex",
                     GM("doSetup", "doCopyRight", null, null),
                     GM("doSetup", "doCopyRight", null, null));
  public static final MappingSet COMPARE_MAPPING =
      new MappingSet("leftIndex", "rightIndex",
                     GM("doSetup", "doCompare", null, null),
                     GM("doSetup", "doCompare", null, null));
  public static final MappingSet COMPARE_RIGHT_MAPPING =
      new MappingSet("rightIndex", "null",
                     GM("doSetup", "doCompare", null, null),
                     GM("doSetup", "doCompare", null, null));
  public static final MappingSet COMPARE_LEFT_MAPPING =
      new MappingSet("leftIndex", "null",
                     GM("doSetup", "doCompareNextLeftKey", null, null),
                     GM("doSetup", "doCompareNextLeftKey", null, null));
  public static final MappingSet COMPARE_NEXT_LEFT_MAPPING =
      new MappingSet("nextLeftIndex", "null",
                     GM("doSetup", "doCompareNextLeftKey", null, null),
                     GM("doSetup", "doCompareNextLeftKey", null, null));

  public void doSetup(@Named("context") FragmentContext context,
                      @Named("status") JoinStatus status,
                      @Named("outgoing") VectorContainer outgoing) throws SchemaChangeException;

  public int doCompare(@Named("leftIndex") int leftIndex,
                       @Named("rightIndex") int rightIndex);

  public int doCompareNextLeftKey(@Named("leftIndex") int leftIndex);

  public boolean doCopyLeft(@Named("leftIndex") int leftIndex,
                            @Named("outIndex") int outIndex);

  public boolean doCopyRight(@Named("rightIndex") int rightIndex,
                             @Named("outIndex") int outIndex);

}
