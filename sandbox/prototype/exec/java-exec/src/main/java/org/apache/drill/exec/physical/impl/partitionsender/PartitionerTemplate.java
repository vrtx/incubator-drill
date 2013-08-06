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
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

import com.google.common.collect.ImmutableList;

import java.util.List;

public abstract class PartitionerTemplate implements Partitioner {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionerTemplate.class);

  private ImmutableList<TransferPair> transfers;
  private SelectionVector2 vector2;
  private SelectionVector4 vector4;
  private SelectionVectorMode svMode;

  public PartitionerTemplate() throws SchemaChangeException {
  }

  @Override
  public final int processBatch(final int recordCount, int firstOutputIndex) {
    switch (svMode) {
      case FOUR_BYTE:
        throw new UnsupportedOperationException();
      case TWO_BYTE:
        final int count = recordCount*2;
        for (int i = 0; i < count; i+=2, firstOutputIndex++) {
          doEval(vector2.getIndex(i), firstOutputIndex);
        }
        return recordCount;
      case NONE:
        final int countN = recordCount;
        for (int i = 0; i < countN; i++, firstOutputIndex++) {
          doEval(i, firstOutputIndex);
        }
        return recordCount;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public final void setup(FragmentContext context,
                          RecordBatch incoming,
                          HashPartitionSender operator) throws SchemaChangeException {
    this.svMode = incoming.getSchema().getSelectionVectorMode();
    switch (svMode) {
      case FOUR_BYTE:
        this.vector4 = incoming.getSelectionVector4();
        break;
      case TWO_BYTE:
        this.vector2 = incoming.getSelectionVector2();
        break;
    }
    // 
    setupEval(context, incoming, operator);
  }

  @Override
  public void partitionBatch(RecordBatch incoming, List<OutgoingRecordBatch> outgoing) {
    // populate outgoing batches
    //    - eval input record to determine outgoing batch
    //    - copy row into computed outgoing batch
    //    - if outgoing batch is full, send it
  }

  protected abstract void setupEval(FragmentContext context,
                                    RecordBatch incoming,
                                    HashPartitionSender operator) throws SchemaChangeException;
  protected abstract void doEval(int inIndex, int outIndex);

}
