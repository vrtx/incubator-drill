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

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.common.expression.*;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.BigIntHolder;
import org.apache.drill.exec.vector.IntHolder;
import org.apache.drill.exec.vector.VarCharHolder;

@FunctionTemplate(name = "hash", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class Hash implements DrillFunc {

  @Param BigIntHolder in;
  @Output IntHolder out;

  public void setup(RecordBatch incoming) {
  }

  public void eval() {
    System.out.println("[GENERATED: HASH]: hash() eval called.");
    // TODO: implement actual hash function (e.g. murmur3), on multiple input types.
    out.value = (int)in.value;
  }

  public static class Provider implements CallProvider{

    @Override
    public FunctionDefinition[] getFunctionDefintions() {
      return new FunctionDefinition[] {
          FunctionDefinition.simple("hash",
                                    new ArgumentValidators.AnyTypeAllowed(1),
                                    OutputTypeDeterminer.FIXED_INT,
                                    "hash")
      };
    }

  }
}


