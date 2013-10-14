package org.apache.drill.exec.physical.impl.mergereceiver;

/**
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
 */

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.record.RawFragmentBatchProvider;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

import java.util.Iterator;


public class MergingRecordBatch implements RecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MergingRecordBatch.class);

  private RecordBatchLoader batchLoader;
  private RawFragmentBatchProvider[] fragProviders;
  private FragmentContext context;
  private BatchSchema schema;


  public MergingRecordBatch(FragmentContext context, RawFragmentBatchProvider[] fragProviders) {
    this.fragProviders = fragProviders;
    this.context = context;
    this.batchLoader = new RecordBatchLoader(context.getAllocator());

    for (RawFragmentBatchProvider provider : fragProviders)
      logger.info(" + Adding fragment provider to MergingRecordBatch {} ", provider);

  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

  @Override
  public int getRecordCount() {
    return batchLoader.getRecordCount();
  }

  @Override
  public void kill() {
    for (RawFragmentBatchProvider provider : fragProviders) {
      provider.kill(context);
    }
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return batchLoader.iterator();
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
    return batchLoader.getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(int fieldId, Class<?> clazz) {
    return batchLoader.getValueAccessorById(fieldId, clazz);
  }

  @Override
  public IterOutcome next() {
    // TODO: properly handle schema changes
    // ...

    for (RawFragmentBatchProvider provider : fragProviders) {
      RawFragmentBatch batch = provider.getNext();
      try{
        if (batch == null) return IterOutcome.NONE;
  
        logger.debug("Next received batch {}", batch);
  
        UserBitShared.RecordBatchDef rbd = batch.getHeader().getDef();
        boolean schemaChanged = batchLoader.load(rbd, batch.getBody());
        batch.release();
        if(schemaChanged){
          this.schema = batchLoader.getSchema();
          return IterOutcome.OK_NEW_SCHEMA;
        }else{
          return IterOutcome.OK;
        }
      }catch(SchemaChangeException ex){
        context.fail(ex);
        return IterOutcome.STOP;
      }
    }

    // TODO:
    return IterOutcome.OK_NEW_SCHEMA;

  }

  @Override
  public WritableBatch getWritableBatch() {
    return batchLoader.getWritableBatch();
  }

}