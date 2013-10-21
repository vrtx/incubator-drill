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

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.DeadBuf;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.record.RawFragmentBatchProvider;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.SchemaBuilder;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

import java.lang.Class;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;


public class MergingRecordBatch implements RecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MergingRecordBatch.class);

  private RecordBatchLoader[] batchLoaders;
  private RawFragmentBatchProvider[] fragProviders;
  private FragmentContext context;
  private BatchSchema schema;
  private VectorContainer outgoingContainer;

  private boolean hasRun = false;
  private final int senderCount;
  private final RawFragmentBatch[] incomingBatches;
  private final VectorWrapper[] incomingVectors;
  private int[] batchOffsets;
  private PriorityQueue <Node> pqueue;

  public MergingRecordBatch(FragmentContext context, RawFragmentBatchProvider[] fragProviders) {
    this.fragProviders = fragProviders;
    this.context = context;

    // initialize arrays of incoming batches, vectors, counters, etc.
    this.senderCount = fragProviders.length;
    this.incomingVectors = new VectorWrapper[senderCount];
    this.incomingBatches = new RawFragmentBatch[senderCount];
    this.batchOffsets = new int[senderCount];
    this.pqueue = new PriorityQueue<Node>(fragProviders.length, new Comparator<Node>() {
      public int compare(Node node1, Node node2) {
        if (node1.valueIndex < node2.valueIndex) return -1;
        if (node1.valueIndex > node2.valueIndex) return 1;
        return 0;
      }
    });
    this.outgoingContainer = new VectorContainer();

    // allocate the incoming record batch loaders (loaded in next())
    this.batchLoaders = new RecordBatchLoader[senderCount];
    for (int i = 0; i < senderCount; ++i)
      batchLoaders[i] = new RecordBatchLoader(context.getAllocator());

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
    if (!outgoingContainer.iterator().hasNext())
      return 0;
    return outgoingContainer.iterator().next().getValueVector().getAccessor().getValueCount();
  }

  @Override
  public void kill() {
    for (RawFragmentBatchProvider provider : fragProviders) {
      provider.kill(context);
    }
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return outgoingContainer.iterator();
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
    return outgoingContainer.getValueVector(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(int fieldId, Class<?> clazz) {
    return outgoingContainer.getValueAccessorById(fieldId, clazz);
  }
  
  private class Node {
    int batchId;
    int valueIndex;
    public Node(int batchId, int valueIndex) {
      this.batchId = batchId;
      this.valueIndex = valueIndex;
    }
  }

  @Override
  public IterOutcome next() {

    // invariants
    if (fragProviders.length == 0) return IterOutcome.NONE;
    // TODO: temp testing
    final SchemaPath path = new SchemaPath("blue", ExpressionPosition.UNKNOWN);
    final Class <BigIntVector> clazz = BigIntVector.class;

    boolean schemaChanged = false;

    if (!hasRun) {
      schemaChanged = true; // first iteration is always a schema change
      
      // set up each incoming record batch
      int batchCount = 0;
      for (RawFragmentBatchProvider provider : fragProviders) {
        incomingBatches[batchCount] = provider.getNext();
        logger.info(" + Adding fragment provider to MergingRecordBatch with id {}", batchCount);
        ++batchCount;
      }

      int i = 0;
      for (RawFragmentBatch batch : incomingBatches) {
        // initialize the incoming batchLoaders
        UserBitShared.RecordBatchDef rbd = batch.getHeader().getDef();
        try {
          batchLoaders[i].load(rbd, batch.getBody());
        } catch(SchemaChangeException ex) {
          // NOTE: operators generally don't throw an SCE unless an error occurs.  May be unrelated to IterOutcome.
          context.fail(ex);
          return IterOutcome.STOP;
        }
        batch.release();
        ++batchOffsets[i];
        ++i;
      }

      // generate the outgoing vector container, arbitrarily based on the first sender's first batch
      SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE);
      int recordCapacity = batchLoaders[0].getRecordCount();
      logger.debug("Allocating {} new outgoing vectors based on the initial batch", recordCapacity);
      for (VectorWrapper<?> v : batchLoaders[0]) {
        // add field to the output schema
        bldr.addField(v.getField());

        // allocate a new value vector
        ValueVector outgoingVector = TypeHelper.getNewVector(v.getField(), context.getAllocator());
        VectorAllocator.getAllocator(v.getValueVector(), outgoingVector).alloc(recordCapacity);
        outgoingContainer.add(outgoingVector);
      }
      BatchSchema batchSchema = bldr.build();
      if (batchSchema != null && !batchSchema.equals(schema)) {
        // TODO: handle case where one or more batches indicate schema change
        logger.debug("Initial state has incoming batches with different schemas");
      }
      schema = batchSchema;

      // populate the priority queue with initial values
      int batchId = 0;
      for (RecordBatchLoader loader : batchLoaders) {
        Node value = new Node(batchId, 0);
        //loader.getValueAccessorById(loader.getValueVectorId(path).getFieldId(), clazz)
        //  .getValueVector()
        //  .getAccessor()
        //  .getObject(0));
        pqueue.add(value);
        ++batchId;
      }

      hasRun = true;
    }

    while (!pqueue.isEmpty()) {
      // pop next value from pq and copy to outgoing batch
      Node node = pqueue.peek();
      copyRecordToOutgoingBatch(pqueue.poll(), outgoingContainer);
      if (node.valueIndex == batchLoaders[node.batchId].getRecordCount()) {
        // reached the end of a record batch
        incomingBatches[node.batchId] = fragProviders[node.batchId].getNext();
        if (incomingBatches[node.batchId].getHeader().getIsLastBatch()) {
          // batch is empty
          incomingBatches[node.batchId].release();
          boolean allBatchesEmpty = true;
          for (RawFragmentBatch batch : incomingBatches) {
            // see if all batches are empty (time to return IterOutcome.NONE)
            if (!batch.getHeader().getIsLastBatch()) {
              allBatchesEmpty = false;
              break;
            }
          }
          if (allBatchesEmpty)
            return IterOutcome.NONE;
          // this batch is empty and will not be used again since the pqueue no longer contains a reference
          continue;
        }
        UserBitShared.RecordBatchDef rbd = incomingBatches[node.batchId].getHeader().getDef();
        try {
            batchLoaders[node.batchId].load(rbd, incomingBatches[node.batchId].getBody());
          } catch(SchemaChangeException ex) {
            context.fail(ex);
            return IterOutcome.STOP;
          }
        incomingBatches[node.batchId].release();
        batchOffsets[node.batchId] = 0;
        // add front value from batch[x] to priority queue
        if (batchLoaders[node.batchId].getRecordCount() != 0)
          pqueue.add(new Node(node.batchId, 0));
      } else {
        pqueue.add(new Node(node.batchId, node.valueIndex + 1));
      }
    }
    // TODO: if outgoing is full, return OK/OK_NEW_SCHEMA and maintain state!

    if (schemaChanged)
      return IterOutcome.OK_NEW_SCHEMA;
    else
      return IterOutcome.OK;
  }

  private void copyRecordToOutgoingBatch(Node node, VectorContainer outgoing) {
    System.out.println(" + Copying value.  batch: [" + node.batchId + "], rowIndex: [" + node.valueIndex + "]");
  }

  private void setupNewSchema() {

  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

}