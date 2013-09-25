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
package org.apache.drill.exec.physical.impl.sort;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.apache.drill.common.defs.OrderDef;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.logical.data.Order.Direction;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.CodeGenerator.HoldingContainer;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.HoldingContainerExpression;
import org.apache.drill.exec.expr.fn.impl.ComparatorFunctions;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

import com.google.common.collect.ImmutableList;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;

public class SortBatch extends AbstractRecordBatch<Sort> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SortBatch.class);
  static int f = 0;
  public static final MappingSet MAIN_MAPPING = new MappingSet( (String) null, null, CodeGenerator.DEFAULT_SCALAR_MAP, CodeGenerator.DEFAULT_SCALAR_MAP);
  public static final MappingSet LEFT_MAPPING = new MappingSet("leftIndex", null, CodeGenerator.DEFAULT_SCALAR_MAP, CodeGenerator.DEFAULT_SCALAR_MAP);
  public static final MappingSet RIGHT_MAPPING = new MappingSet("rightIndex", null, CodeGenerator.DEFAULT_SCALAR_MAP, CodeGenerator.DEFAULT_SCALAR_MAP);

  private static long MAX_SORT_BYTES = 8l * 1024 * 1024 * 1024;

  private final RecordBatch incoming;
  private SortRecordBatchBuilder builder;
  private SelectionVector4 sv4;
  private Sorter sorter;
  private BatchSchema schema;
  
  public SortBatch(Sort popConfig, FragmentContext context, RecordBatch incoming) {
    super(popConfig, context);
    this.incoming = incoming;
  }

  @Override
  public int getRecordCount() {
    //return sv4.getCount();
    return sv4 != null ? sv4.getCount() : 0;
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
    return sv4;
  }

  
  
  @Override
  protected void cleanup() {
    super.cleanup();
    container.zeroVectors();;
    sv4.clear();
  }

  @Override
  public IterOutcome next() {
    if(builder != null){
      if(sv4.next()){
        return IterOutcome.OK;
      }else{
        return IterOutcome.NONE;
      }
    }
    
    
    try{
      outer: while (true) {
        IterOutcome upstream = incoming.next();
        switch (upstream) {
        case NONE:
          logger.warn("SortBatch upstream NONE.  incoming schema: {}, current schema: {}", incoming.getSchema(), schema);
          //if (!incoming.getSchema().equals(schema)){
          if (schema == null) {
            if (builder != null) throw new UnsupportedOperationException("Sort doesn't currently support sorts with changing schemas.");
            builder = new SortRecordBatchBuilder(context.getAllocator(), MAX_SORT_BYTES, container);
            System.out.println("SortBatch: Incoming batch iter returned NONE with schemas: " + incoming.getSchema() + "\n" + schema);
            if(!builder.add(incoming)){
              throw new UnsupportedOperationException("Sort doesn't currently support doing an external sort.");
            };
            //this.schema = incoming.getSchema();
          }
          break outer;
        case NOT_YET:
        case STOP:
          container.zeroVectors();
          return upstream;
        case OK_NEW_SCHEMA:
          // only change in the case that the schema truly changes.  Artificial schema changes are ignored.
          if(!incoming.getSchema().equals(schema)){
            if (builder != null) throw new UnsupportedOperationException("Sort doesn't currently support sorts with changing schemas.");
            builder = new SortRecordBatchBuilder(context.getAllocator(), MAX_SORT_BYTES, container);
            this.schema = incoming.getSchema();
          }
          // fall through.
        case OK:
          //System.out.println("Adding batch to sorter with schema " + incoming.getSchema());
          for (VectorWrapper w : incoming) {
            // for each column
            Map<Long, Boolean> values = new HashedMap();
            if (w.getField().matches(new SchemaPath("NATIONKEY", ExpressionPosition.UNKNOWN))) {
              for (int i = 0; i < incoming.getRecordCount(); i++) {
                if (values.get((Long)w.getValueVector().getAccessor().getObject(i)) == null) {
                  values.put((Long)w.getValueVector().getAccessor().getObject(i), Boolean.TRUE);
                }
                //System.out.print(" [" + w.getValueVector().getAccessor().getObject(i) + "]");
                //if (i % 10 == 0) System.out.println();
              }
              
              System.out.println(f++ + ": Total unique values in this sort batch: " + values.size());
            }
          }

          if(!builder.add(incoming)){
            throw new UnsupportedOperationException("Sort doesn't currently support doing an external sort.");
          };
          break;
        default:
          throw new UnsupportedOperationException();
        }
      }

      builder.build(context);
      if (builder.getBatchCount() == 0) {
        // if no batches were received, return an outcome of NONE.
        System.out.println("Schema for batch with 0 entries: " + schema + ", container: " + container);
        container.setSchema(schema);
        return IterOutcome.NONE;
      }
      sv4 = builder.getSv4();

      sorter = createNewSorter();
      sorter.setup(context, this);
      long t1 = System.nanoTime();
      sorter.sort(sv4, container);
      logger.debug("Sorted {} records in {} micros.", sv4.getTotalCount(), (System.nanoTime() - t1)/1000);
      
      return IterOutcome.OK_NEW_SCHEMA;
      
    }catch(SchemaChangeException | ClassTransformationException | IOException ex){
      kill();
      logger.error("Failure during query", ex);
      context.fail(ex);
      return IterOutcome.STOP;
    }
  }

  
  
  private Sorter createNewSorter() throws ClassTransformationException, IOException, SchemaChangeException{
    CodeGenerator<Sorter> g = new CodeGenerator<Sorter>(Sorter.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    g.setMappingSet(MAIN_MAPPING);
    
    for(OrderDef od : popConfig.getOrderings()){
      // first, we rewrite the evaluation stack for each side of the comparison.
      ErrorCollector collector = new ErrorCollectorImpl(); 
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), this, collector);
      if(collector.hasErrors()) throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      g.setMappingSet(LEFT_MAPPING);
      HoldingContainer left = g.addExpr(expr, false);
      g.setMappingSet(RIGHT_MAPPING);
      HoldingContainer right = g.addExpr(expr, false);
      g.setMappingSet(MAIN_MAPPING);
      
      // next we wrap the two comparison sides and add the expression block for the comparison.
      FunctionCall f = new FunctionCall(ComparatorFunctions.COMPARE_TO, ImmutableList.of((LogicalExpression) new HoldingContainerExpression(left), new HoldingContainerExpression(right)), ExpressionPosition.UNKNOWN);
      HoldingContainer out = g.addExpr(f, false);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));
      
      //TODO: is this the right order...
      if(od.getDirection() == Direction.ASC){
        jc._then()._return(out.getValue());
      }else{
        jc._then()._return(out.getValue().minus());
      }
    }
    
    g.getEvalBlock()._return(JExpr.lit(0));
    
    return context.getImplementationClass(g);


  }
  
  @Override
  public WritableBatch getWritableBatch() {
    throw new UnsupportedOperationException("A sort batch is not writable.");
  }

  @Override
  protected void killIncoming() {
    incoming.kill();
  }

  
  

}
