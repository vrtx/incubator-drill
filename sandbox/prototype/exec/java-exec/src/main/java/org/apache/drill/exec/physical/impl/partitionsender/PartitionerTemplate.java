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

package org.apache.drill.exec.physical.impl.partitionsender;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

public abstract class PartitionerTemplate implements Partitioner {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionerTemplate.class);

  private List<OutgoingRecordBatch> outgoing;
  
  public PartitionerTemplate() throws SchemaChangeException {
  }

  @Override
  public final void setup(FragmentContext context,
                          RecordBatch incoming,
                          List<OutgoingRecordBatch> outgoing) throws SchemaChangeException {
    this.outgoing = outgoing;
    doSetup(context, incoming, outgoing.get(0));  // TODO
  }

  @Override
  public void partitionBatch(RecordBatch incoming) {
    // TODO: populate outgoing batches
    //    - eval input record to determine outgoing batch
    //    - copy row into computed outgoing batch
    //    - if outgoing batch is full, send it
                  //    final int countN = recordCount;
                  //    for (int i = 0; i < countN; i++, firstOutputIndex++) {
                  //      doEval(i, firstOutputIndex);
                  //    }
                  //    return recordCount;
  }

  protected abstract void doSetup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing) throws SchemaChangeException;
  protected abstract void doEval(int inIndex, int outIndex);

}
