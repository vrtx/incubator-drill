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

package org.apache.drill.exec.physical.impl.hashsender;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.physical.impl.RootCreator;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.record.RecordBatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PartitionSenderCreator implements RootCreator<HashPartitionSender> {

  @Override
  public RootExec getRoot(FragmentContext context,
                          HashPartitionSender config,
                          List<RecordBatch> children) throws ExecutionSetupException {
    assert children != null && children.size() == 1;
    return new PartitionSenderRootExec(context, children.iterator().next(), config);
  }

  private static class PartitionSenderRootExec implements RootExec {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionSenderRootExec.class);
    private RecordBatch incoming;
    private HashPartitionSender operator;
    private List<OutgoingRecordBatch> outgoing;
    private Partitioner partitioner;
    private FragmentContext context;
    private volatile boolean ok = true;

    public PartitionSenderRootExec(FragmentContext context, RecordBatch incoming, HashPartitionSender operator) {
      this.incoming = incoming;
      this.operator = operator;
      this.context = context;
      this.outgoing = new ArrayList<>();
      for (DrillbitEndpoint endpoint : operator.getDestinations()) {
        outgoing.add(new OutgoingRecordBatch(operator, context.getCommunicator().getTunnel(endpoint), incoming, context));
      }
    }

    @Override
    public boolean next() {
      if (!ok) {
        incoming.kill();
        return false;
      }

      RecordBatch.IterOutcome out = incoming.next();
      logger.debug("Partitioner.next() sending status {}", out);
      switch(out){
        case STOP:
        case NONE:
          if (partitioner == null)
            return false;

          // populate outgoing batches
          if (incoming.getRecordCount() > 0)
            partitioner.partitionBatch(incoming, outgoing);

          // send all pending batches
          flushOutgoingBatches(true);
          return false;

        case OK_NEW_SCHEMA:
          // send all existing batches
          flushOutgoingBatches(false);
          // update OutgoingRecordBatch's schema
          try {
            createPartitioner();
          } catch (SchemaChangeException e) {
            incoming.kill();
            logger.error("Failure to create partitioning sender during query ", e);
            context.fail(e);
            return false;
          }
        case OK:
          partitioner.partitionBatch(incoming, outgoing);
          return true;
        case NOT_YET:
        default:
          throw new IllegalStateException();
      }
    }

    private void createPartitioner() throws SchemaChangeException {
      final LogicalExpression expr = operator.getExpr();
      final ErrorCollector collector = new ErrorCollectorImpl();
      final CodeGenerator<Partitioner> cg = new CodeGenerator<Partitioner>(Partitioner.TEMPLATE_DEFINITION, context.getFunctionRegistry());

      final LogicalExpression logicalExp = ExpressionTreeMaterializer.materialize(expr, incoming, collector);
      if (collector.hasErrors()) {
        throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
      }

      // generate code to copy vectors, similar to svremover's RemovingRecordBatch.generateCopies()

      try {
        // compile and setup generated code
        partitioner = context.getImplementationClass(cg);
        partitioner.setup(context, incoming, operator);
      } catch (ClassTransformationException | IOException e) {
        throw new SchemaChangeException("Failure while attempting to load generated class", e);
      }
    }

    /**
     * Flush each outgoing record batch, and reset the state of each batch (e.g. due to schema change).  Note that
     * the schema is updated based on incoming when called.
     * @param isLastBatch
     */
    public void flushOutgoingBatches(boolean isLastBatch) {
      for (OutgoingRecordBatch batch : outgoing) {
        if (isLastBatch)
          batch.setIsLast();
        batch.flush();
        batch.resetBatch();
      }
    }

    @Override
    public void stop() {
      ok = false;
    }
  }
}