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

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JVar;
import org.apache.drill.common.expression.*;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.fn.impl.ComparatorFunctions;
import org.apache.drill.exec.expr.fn.impl.Hash;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.physical.impl.filter.ReturnValueExpression;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class PartitionSenderRootExec implements RootExec {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionSenderRootExec.class);
  private RecordBatch incoming;
  private HashPartitionSender operator;
  private List<OutgoingRecordBatch> outgoing;
  private Partitioner partitioner;
  private FragmentContext context;
  private boolean ok = true;

  public PartitionSenderRootExec(FragmentContext context,
                                 RecordBatch incoming,
                                 HashPartitionSender operator) {

    this.incoming = incoming;
    this.operator = operator;
    this.context = context;
    this.outgoing = new ArrayList<>();
    for (CoordinationProtos.DrillbitEndpoint endpoint : operator.getDestinations())
      outgoing.add(new OutgoingRecordBatch(operator,
        context.getCommunicator().getTunnel(endpoint),
        incoming,
        context));
    try {
      createPartitioner();
    } catch (SchemaChangeException e) {
      ok = false;
      logger.error("Failure to create partitioning sender during query ", e);
      context.fail(e);
    }
  }

  @Override
  public boolean next() {

    if (!ok) {
      stop();
      return false;
    }

    RecordBatch.IterOutcome out = incoming.next();
    logger.debug("Partitioner.next(): got next record batch with status {}", out);
    switch(out){
      case STOP:
      case NONE:
        System.out.println("\n[STOP | NONE]");
        System.out.println("(STOP | NONE) sending batch with incoming record count: " + incoming.getRecordCount());
        // populate outgoing batches
        if (incoming.getRecordCount() > 0)
          partitioner.partitionBatch(incoming);

        // send all pending batches
        System.out.println("(STOP | NONE) flushing LAST batch; schema change = false.  outgoing vectors: " + outgoing.size());
        flushOutgoingBatches(true, false);
        return false;

      case OK_NEW_SCHEMA:
        // send all existing batches
        System.out.println("\n[OK_NEW_SCHEMA]");
        System.out.println("(OK_NEW_SCHEMA) flushing all record batches.  outgoing vectors: " + outgoing.size());
        flushOutgoingBatches(false, true);

        // update OutgoingRecordBatch's schema and value vectors
        try {
          partitioner.setup(context, incoming, outgoing);
          System.out.println("(OK_NEW_SCHEMA) done setting up partitioner.  outgoing vectors: " + outgoing.size());
        } catch (SchemaChangeException e) {
          System.out.println("(OK_NEW_SCHEMA) EXCEPTION: " + e);
          incoming.kill();
          logger.error("Failure to create partitioning sender during query ", e);
          context.fail(e);
          return false;
        }
      case OK:
        System.out.println("\n[OK]");
        System.out.println("(OK) partitioning incoming record batch among " + outgoing.size() + " outgoing batches");
        partitioner.partitionBatch(incoming);
        return true;
      case NOT_YET:
      default:
        throw new IllegalStateException();
    }
  }

  public void stop() {
    ok = false;
    incoming.kill();
  }

  private void createPartitioner() throws SchemaChangeException {

    // set up partitioning function
    final LogicalExpression expr = operator.getExpr();
    System.out.println("Creating Partitioner w/ expression: " + expr);
    final ErrorCollector collector = new ErrorCollectorImpl();
    final CodeGenerator<Partitioner> cg = new CodeGenerator<Partitioner>(Partitioner.TEMPLATE_DEFINITION, context.getFunctionRegistry());

    final LogicalExpression logicalExp = ExpressionTreeMaterializer.materialize(expr, incoming, collector);
    if (collector.hasErrors()) {
      throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
    }

    // generate code to copy from an incoming value vector to the destination partition's outgoing value vector
    int fieldId = 0;
    JExpression inIndex = JExpr.direct("inIndex");
    JExpression outIndex = JExpr.direct("outIndex");
    cg.rotateBlock();

    List<JVar> outVVs = Lists.newArrayList();
    for (OutgoingRecordBatch batch : outgoing) {
      for (VectorWrapper<?> vv : batch) {
        outVVs.add(cg.declareVectorValueSetupAndMember("outgoing", new TypedFieldId(vv.getField().getType(), fieldId, false)));
        fieldId++;
      }
    }
    for (VectorWrapper<?> vvIn : incoming) {
      JVar inVV = cg.declareVectorValueSetupAndMember("incoming", new TypedFieldId(vvIn.getField().getType(), fieldId, vvIn.isHyper()));
      cg.getBlock().add(outVVs.get(0).invoke("copyFrom").arg(inIndex).arg(outIndex).arg(inVV));
    }

    try {
      // compile and setup generated code
      partitioner = context.getImplementationClass(cg);
      partitioner.setup(context, incoming, outgoing);

    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }
  
  /**
   * Flush each outgoing record batch, and optionally reset the state of each outgoing record
   * batch (on schema change).  Note that the schema is updated based on incoming at the time
   * this function is invoked.
   *
   * @param isLastBatch    true if this is the last incoming batch
   * @param schemaChanged  true if the schema has changed
   */
  public void flushOutgoingBatches(boolean isLastBatch, boolean schemaChanged) {
    for (OutgoingRecordBatch batch : outgoing) {
      System.out.println("flushOutgoingBatches: flushing batch: " + (isLastBatch ? "LastBatch " : "")
                                                                  + (schemaChanged ? "schemaChanged " : "")
                                                                  + batch);
      if (isLastBatch) batch.setIsLast();
      batch.flush();
      if (schemaChanged) batch.resetBatch();
    }
  }
}
