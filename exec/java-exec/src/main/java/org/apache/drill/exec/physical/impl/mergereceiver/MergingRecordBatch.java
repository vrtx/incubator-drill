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

import com.google.common.collect.Lists;
import com.sun.codemodel.JArray;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MergingReceiverPOP;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.BatchSchema;
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
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

import java.io.IOException;
import java.lang.Class;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;


public class MergingRecordBatch implements RecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MergingRecordBatch.class);

  private RecordBatchLoader[] batchLoaders;
  private RawFragmentBatchProvider[] fragProviders;
  private FragmentContext context;
  private BatchSchema schema;
  private VectorContainer outgoingContainer;
  private MergingReceiverGeneratorBase merger;
  private boolean hasRun = false;
  private boolean lastBatchWasFull = false;
  private final int DEFAULT_ALLOC_RECORD_COUNT = 20000;

  private int outgoingPosition = 0;
  private final int senderCount;
  private final RawFragmentBatch[] incomingBatches;
  private final VectorWrapper[] incomingVectors;
  private int[] batchOffsets;
  private PriorityQueue <Node> pqueue;
  private List<VectorAllocator> allocators;
  private MergingReceiverPOP config;

  public MergingRecordBatch(FragmentContext context,
                            MergingReceiverPOP config,
                            RawFragmentBatchProvider[] fragProviders) {

    this.fragProviders = fragProviders;
    this.context = context;
    this.config = config;
    this.senderCount = fragProviders.length;

    this.incomingVectors = new VectorWrapper[senderCount];
    this.incomingBatches = new RawFragmentBatch[senderCount];
    this.batchOffsets = new int[senderCount];
    this.allocators = Lists.newArrayList();
    this.outgoingContainer = new VectorContainer();

    // allocate the priority queue with the generated comparator
    this.pqueue = new PriorityQueue<Node>(fragProviders.length, new Comparator<Node>() {
      public int compare(Node node1, Node node2) {
        return merger.doCompare(node1.batchId, node1.valueIndex,
                                node2.batchId, node2.valueIndex);
      }
    });

    // allocate the incoming record batch loaders (loading happens in next())
    this.batchLoaders = new RecordBatchLoader[senderCount];
    for (int i = 0; i < senderCount; ++i)
      batchLoaders[i] = new RecordBatchLoader(context.getAllocator());

  }

  @Override
  public IterOutcome next() {
    if (fragProviders.length == 0) return IterOutcome.NONE;
    boolean schemaChanged = false;

    if (!hasRun) {
      schemaChanged = true; // first iteration is always a schema change
      
      // set up each incoming record batch
      int batchCount = 0;
      for (RawFragmentBatchProvider provider : fragProviders) {
        incomingBatches[batchCount] = provider.getNext();
        logger.debug("Adding fragment provider to MergingRecordBatch with id {}", batchCount);
        ++batchCount;
      }

      int i = 0;
      for (RawFragmentBatch batch : incomingBatches) {
        // initialize the incoming batchLoaders
        UserBitShared.RecordBatchDef rbd = batch.getHeader().getDef();
        try {
          batchLoaders[i].load(rbd, batch.getBody());
        } catch(SchemaChangeException e) {
          logger.error("MergingReceiver failed to load record batch from remote host.  {}", e);
          context.fail(e);
          return IterOutcome.STOP;
        }
        batch.release();
        ++batchOffsets[i];
        ++i;
      }

      // create the outgoing schema and vector container, and allocate the initial batch
      SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE);
      int recordCapacity = batchLoaders[0].getRecordCount();
      int vectorCount = 0;
      for (VectorWrapper<?> v : batchLoaders[0]) {

        // add field to the output schema
        bldr.addField(v.getField());

        // allocate a new value vector
        ValueVector outgoingVector = TypeHelper.getNewVector(v.getField(), context.getAllocator());
        VectorAllocator allocator = VectorAllocator.getAllocator(v.getValueVector(), outgoingVector);
        allocator.alloc(recordCapacity);
        allocators.add(allocator);
        outgoingContainer.add(outgoingVector);
        ++vectorCount;
      }
      logger.debug("Allocating {} vectors with {} values", vectorCount, recordCapacity);
      BatchSchema batchSchema = bldr.build();
      if (batchSchema != null && !batchSchema.equals(schema)) {
        // TODO: handle case where one or more batches indicate schema change
        logger.debug("Initial state has incoming batches with different schemas");
      }
      schema = batchSchema;

      // generate code for copy and compare
      try {
        merger = createMerger();
      } catch (SchemaChangeException e) {
        logger.error("Failed to generate code for MergingReceiver.  {}", e);
        context.fail(e);
        return IterOutcome.STOP;
      }
      // populate the priority queue with initial values
      int batchId = 0;
      for (RecordBatchLoader loader : batchLoaders) {
        Node value = new Node(batchId, 0);
        pqueue.add(value);
        ++batchId;
      }

      hasRun = true;
    }

    if (lastBatchWasFull) {
      logger.debug("Outgoing vectors were full on last iteration; resetting.");
      allocateOutgoing();
      outgoingPosition = 0;
      lastBatchWasFull = false;
    }

    while (!pqueue.isEmpty()) {
      // pop next value from pq and copy to outgoing batch
      Node node = pqueue.peek();
      copyRecordToOutgoingBatch(pqueue.poll());

      if (isOutgoingFull()) {
        // set a flag so that we reallocate on the next iteration
        logger.debug("Outgoing vectors are full; breaking");
        lastBatchWasFull = true;
        break;
      }

      if (node.valueIndex == batchLoaders[node.batchId].getRecordCount()) {
        // reached the end of an incoming record batch
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

          // this batch is empty; since the pqueue no longer references this batch, it will be
          // ignored in subsequent iterations.
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

    if (schemaChanged)
      return IterOutcome.OK_NEW_SCHEMA;
    else
      return IterOutcome.OK;
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

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  private void allocateOutgoing() {
    for (VectorAllocator allocator : allocators) {
      allocator.alloc(DEFAULT_ALLOC_RECORD_COUNT);
    }
  }

  private boolean isOutgoingFull() {
    return outgoingPosition == getRecordCount();
  }

  /**
   * Generate code for the merger's copy and compare methods.
   * 
   * @return instance of a new merger based on generated code
   * @throws SchemaChangeException
   */
  private MergingReceiverGeneratorBase createMerger() throws SchemaChangeException {

    // set up the merger's comparison expression
    final LogicalExpression expr = config.getExpression();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final CodeGenerator<MergingReceiverGeneratorBase> cg =
        new CodeGenerator<>(MergingReceiverGeneratorBase.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    final LogicalExpression materializedExpr = ExpressionTreeMaterializer.materialize(expr, this, collector);
    if (collector.hasErrors()) {
      throw new SchemaChangeException(String.format(
        "Failure while trying to materialize incoming schema.  Errors:\n %s.",
        collector.toErrorString()));
    }

    // generate code to copy values from an incoming batch to the outgoing vector container
    JExpression inIndex = JExpr.direct("inIndex");
    JType outgoingVectorArrayType = cg.getModel().ref(ValueVector.class).array().array();

    // generate evaluation expression to sort incoming records
    CodeGenerator.HoldingContainer exprHolder = cg.addExpr(materializedExpr);

////    // declare a two-dimensional array of value vectors; batch is first dimension, ValueVector is the second
////    JVar outgoingVectors = cg.clazz.field(JMod.NONE,
////      outgoingVectorArrayType,
////      "outgoingVectors");
//
//    // create 2d array and build initialization list.  For example:
//    //     outgoingVectors = new ValueVector[][] {
//    //                              new ValueVector[] {vv1, vv2},
//    //                              new ValueVector[] {vv3, vv4}
//    //                       });
//    JArray outgoingVectorInit = JExpr.newArray(cg.getModel().ref(ValueVector.class).array());
//
//    int fieldId = 0;
//    int batchId = 0;
//    for (RecordBatchLoader batch : batchLoaders) {
//      JArray outgoingVectorInitBatch = JExpr.newArray(cg.getModel().ref(ValueVector.class));
//      for (VectorWrapper<?> vv : batch) {
//        // declare outgoing value vector and assign it to the array
//        JVar outVV = cg.declareVectorValueSetupAndMember("outgoing[" + batchId + "]",
//          new TypedFieldId(vv.getField().getType(),
//            fieldId,
//            false));
//        // add vv to initialization list (e.g. { vv1, vv2, vv3 } )
//        outgoingVectorInitBatch.add(outVV);
//        ++fieldId;
//      }
//
//      // add VV array to initialization list (e.g. new ValueVector[] { ... })
//      outgoingVectorInit.add(outgoingVectorInitBatch);
//      ++batchId;
//      fieldId = 0;
//    }
//
//    // generate outgoing value vector 2d array initialization list.
//    cg.getSetupBlock().assign(outgoingVectors, outgoingVectorInit);
//
//    for (VectorWrapper<?> vvIn : outgoingContainer) {
//      // declare incoming value vectors
//      JVar incomingVV = cg.declareVectorValueSetupAndMember("incoming", new TypedFieldId(vvIn.getField().getType(),
//        fieldId,
//        vvIn.isHyper()));
//
//      // generate the copyFrom() invocation with explicit cast to the appropriate type
//      Class<?> vvType = TypeHelper.getValueVectorClass(vvIn.getField().getType().getMinorType(),
//        vvIn.getField().getType().getMode());
//      JClass vvClass = cg.getModel().ref(vvType);
//      // the following block generates calls to copyFrom(); e.g.:
//      // ((IntVector) outgoingVectors[bucket][0]).copyFrom(inIndex,
//      //                                                     outgoingBatches[bucket].getRecordCount(),
//      //                                                     vv1);
//      cg.getEvalBlock().add(
//        ((JExpression) JExpr.cast(vvClass,
//          ((JExpression)
//            outgoingVectors
//              .component(bucket))
//            .component(JExpr.lit(fieldId))))
//          .invoke("copyFrom")
//          .arg(inIndex)
//          .arg(((JExpression) outgoingBatches.component(bucket)).invoke("getRecordCount"))
//          .arg(incomingVV));
//
//      ++fieldId;
//    }
    try {
      // compile and setup generated code
      MergingReceiverGeneratorBase merger = context.getImplementationClass(cg);
      merger.doSetup(context, batchLoaders, this);
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
    return merger;
  }

  /**
   * Copy the record referenced by the supplied node to the next output position.
   * Side Effect: increments outgoing position
   *
   * @param node Reference to the next record to copy from the incoming batches
   */
  private void copyRecordToOutgoingBatch(Node node) {
    System.out.println(" + Copying value.  batch: [" + node.batchId + "], rowIndex: [" + node.valueIndex + "]");
    merger.doCopy(node.batchId, node.valueIndex, outgoingPosition++);
  }

  /**
   * A Node contains a reference to a single value in a specific incoming batch.  It is used
   * as a wrapper for the priority queue.
   */
  private class Node {
    int batchId;      // incoming batch
    int valueIndex;   // value within the batch
    Node(int batchId, int valueIndex) {
      this.batchId = batchId;
      this.valueIndex = valueIndex;
    }
  }

}