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

import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.physical.impl.VectorHolder;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.bit.BitTunnel;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.work.foreman.ErrorHelper;


public class OutgoingRecordBatch implements RecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OutgoingRecordBatch.class);

  private final BitTunnel tunnel;
  private final HashPartitionSender operator;
  private volatile boolean ok = true;
  private boolean isLast = false;
  private final RecordBatch incoming;
  private final FragmentContext context;
  private BatchSchema outSchema;
  private List<ValueVector> valueVectors;
  private VectorHolder vectorHolder;
  private int recordCount;

  public OutgoingRecordBatch(HashPartitionSender operator, BitTunnel tunnel, RecordBatch incoming, FragmentContext context){
    this.incoming = incoming;
    this.context = context;
    this.operator = operator;
    this.tunnel = tunnel;
    resetBatch();
  }
    
  public void flush() {
    final ExecProtos.FragmentHandle handle = context.getHandle();
    FragmentWritableBatch writableBatch = new FragmentWritableBatch(isLast,
                                                                    handle.getQueryId(),
                                                                    handle.getMajorFragmentId(),
                                                                    handle.getMinorFragmentId(),
                                                                    operator.getOppositeMajorFragmentId(),
                                                                    0,
                                                                    incoming.getWritableBatch());
    tunnel.sendRecordBatch(statusHandler, context, writableBatch);
  }

  public void setIsLast() {
    isLast = true;
  }

  public BitTunnel getTunnel() {
    return tunnel;
  }

  RecordBatch getIncoming() {
    return incoming;
  }

  @Override
  public IterOutcome next() {
    assert false;
    return IterOutcome.STOP;
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return valueVectors.iterator();
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    Preconditions.checkNotNull(outSchema);
    return outSchema;
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  @Override
  public void kill() {
    incoming.kill();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return vectorHolder.getValueVector(path);
  }

  @Override
  public <T extends ValueVector> T getValueVectorById(int fieldId, Class<?> clazz) {
    return vectorHolder.getValueVector(fieldId, clazz);
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  public void resetBatch() {
    if (valueVectors != null) {
      for(ValueVector v : valueVectors){
        v.close();
      }
    }
    this.valueVectors = Lists.newArrayList();
    this.vectorHolder = new VectorHolder(valueVectors);

    SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE);
    for(ValueVector v : incoming){
      bldr.addField(v.getField());
    }
    this.outSchema = bldr.build();
  }

  private StatusHandler statusHandler = new StatusHandler();
  private class StatusHandler extends BaseRpcOutcomeListener<GeneralRPCProtos.Ack> {
    RpcException ex;

    @Override
    public void failed(RpcException ex) {
      logger.error("Failure while sending data to user.", ex);
      ErrorHelper.logAndConvertError(context.getIdentity(), "Failure while sending fragment to client.", ex, logger);
      ok = false;
      this.ex = ex;
    }

  }

}
