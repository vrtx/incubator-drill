package org.apache.drill.exec.physical.impl.join;

import java.io.IOException;

import com.sun.codemodel.*;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MergeJoinPOP;
import org.apache.drill.exec.physical.impl.join.JoinWorker.JoinOutcome;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.vector.*;

/**
 * A merge join combining to incoming in-order batches.
 */
public class MergeJoinBatch extends AbstractRecordBatch<MergeJoinPOP> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MergeJoinBatch.class);
  
  private final RecordBatch left;
  private final RecordBatch right;
  private final JoinStatus status;
  private JoinWorker worker;
  private JoinCondition condition;
  public MergeJoinBatchBuilder batchBuilder;
  
  protected MergeJoinBatch(MergeJoinPOP popConfig, FragmentContext context, RecordBatch left, RecordBatch right) {
    super(popConfig, context);
    this.left = left;
    this.right = right;
    this.status = new JoinStatus(left, right, this);
    this.batchBuilder = new MergeJoinBatchBuilder(context, status);
    this.condition = popConfig.getConditions().get(0);
    // currently only one join condition is supported
    assert popConfig.getConditions().size() == 1;
  }

  @Override
  public int getRecordCount() {
    return status.getOutPosition();
  }

  @Override
  public IterOutcome next() {
    
    // we do this in the here instead of the constructor because don't necessary want to start consuming on construction.
    status.ensureInitial();
    
    // loop so we can start over again if we find a new batch was created.
    while(true){

      // if the previous outcome was a change in schema or we sent a batch, we have to set up a new batch.
      if (status.getOutcome() == JoinOutcome.BATCH_RETURNED ||
          status.getOutcome() == JoinOutcome.SCHEMA_CHANGED)
        allocateBatch();

      // reset the output position to zero after our parent iterates this RecordBatch
      if (status.getOutcome() == JoinOutcome.BATCH_RETURNED ||
          status.getOutcome() == JoinOutcome.SCHEMA_CHANGED ||
          status.getOutcome() == JoinOutcome.NO_MORE_DATA)
        status.resetOutputPos();

      boolean first = false;
      if(worker == null){
        try {
          logger.debug("Creating New Worker");
          this.worker = generateNewWorker();
          first = true;
        } catch (ClassTransformationException | IOException e) {
          context.fail(new SchemaChangeException(e));
          kill();
          return IterOutcome.STOP;
        }
      }

      // join until we have a complete outgoing batch
      worker.doJoin(status);

      // get the outcome of the join.
      switch(status.getOutcome()){
      case BATCH_RETURNED:
        // only return new schema if new worker has been setup.
        logger.debug("BATCH RETURNED; returning {}", (first ? "OK_NEW_SCHEMA" : "OK"));
        return first ? IterOutcome.OK_NEW_SCHEMA : IterOutcome.OK;
      case FAILURE:
        kill();
        return IterOutcome.STOP;
      case NO_MORE_DATA:
        logger.debug("NO MORE DATA; returning {}", (status.getOutPosition() > 0 ? "OK" : "NONE"));
        return status.getOutPosition() > 0 ? IterOutcome.OK: IterOutcome.NONE;
      case SCHEMA_CHANGED:
        worker = null;
        if(status.getOutPosition() > 0){
          // if we have current data, let's return that.
          logger.debug("SCHEMA CHANGED; returning {} ", (first ? "OK_NEW_SCHEMA" : "OK"));
          return first ? IterOutcome.OK_NEW_SCHEMA : IterOutcome.OK;
        }else{
          // loop again to rebuild worker.
          continue;
        }
      case WAITING:
        return IterOutcome.NOT_YET;
      default:
        throw new IllegalStateException();
      }
    }
  }

  public void resetBatchBuilder() {
    batchBuilder = new MergeJoinBatchBuilder(context, status);
  }

  public void addRightToBatchBuilder() {
    batchBuilder.add(right);
  }

  @Override
  protected void killIncoming() {
    left.kill();
    right.kill();
  }

  private JoinWorker generateNewWorker() throws ClassTransformationException, IOException{

    final CodeGenerator<JoinWorker> cg = new CodeGenerator<>(JoinWorker.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    final ErrorCollector collector = new ErrorCollectorImpl();
    final LogicalExpression leftFieldExpr = condition.getLeft();
    final LogicalExpression rightFieldExpr = condition.getRight();

    // Generate members and initialization code
    /////////////////////////////////////////

    // declare and assign JoinStatus member
    cg.setMappingSet(JoinInnerSignature.SETUP_MAPPING);
    JClass joinStatusClass = cg.getModel().ref(JoinStatus.class);
    JVar joinStatus = cg.clazz.field(JMod.NONE, joinStatusClass, "status");
    cg.getSetupBlock().assign(JExpr._this().ref(joinStatus), JExpr.direct("status"));

    // declare and assign outgoing VectorContainer member
    JClass vectorContainerClass = cg.getModel().ref(VectorContainer.class);
    JVar outgoingVectorContainer = cg.clazz.field(JMod.NONE, vectorContainerClass, "outgoing");
    cg.getSetupBlock().assign(JExpr._this().ref(outgoingVectorContainer), JExpr.direct("outgoing"));

    // declare and assign incoming left RecordBatch member
    JClass recordBatchClass = cg.getModel().ref(RecordBatch.class);
    JVar incomingLeftRecordBatch = cg.clazz.field(JMod.NONE, recordBatchClass, "incomingLeft");
    cg.getSetupBlock().assign(JExpr._this().ref(incomingLeftRecordBatch), joinStatus.ref("left"));

    // declare and assign incoming right RecordBatch member
    JVar incomingRightRecordBatch = cg.clazz.field(JMod.NONE, recordBatchClass, "incomingRight");
    cg.getSetupBlock().assign(JExpr._this().ref(incomingRightRecordBatch), joinStatus.ref("right"));

    // declare 'incoming' member so VVReadExpr generated code can point to the left or right batch
    JVar incomingRecordBatch = cg.clazz.field(JMod.NONE, recordBatchClass, "incoming");

    // materialize value vector readers from join expression
    final LogicalExpression materializedLeftExpr = ExpressionTreeMaterializer.materialize(leftFieldExpr, left, collector);
    if (collector.hasErrors())
      throw new ClassTransformationException(String.format(
          "Failure while trying to materialize incoming left field.  Errors:\n %s.", collector.toErrorString()));

    final LogicalExpression materializedRightExpr = ExpressionTreeMaterializer.materialize(rightFieldExpr, right, collector);
    if (collector.hasErrors())
      throw new ClassTransformationException(String.format(
          "Failure while trying to materialize incoming right field.  Errors:\n %s.", collector.toErrorString()));


    // generate compare()
    ////////////////////////
    cg.setMappingSet(JoinInnerSignature.COMPARE_MAPPING);
    cg.getSetupBlock().assign(JExpr._this().ref(incomingRecordBatch), JExpr._this().ref(incomingLeftRecordBatch));
    CodeGenerator.HoldingContainer compareLeftExprHolder = cg.addExpr(materializedLeftExpr, false);
    cg.setMappingSet(JoinInnerSignature.COMPARE_RIGHT_MAPPING);
    cg.getSetupBlock().assign(JExpr._this().ref(incomingRecordBatch), JExpr._this().ref(incomingRightRecordBatch));
    CodeGenerator.HoldingContainer compareRightExprHolder = cg.addExpr(materializedRightExpr, false);

    if (compareLeftExprHolder.isOptional() || compareRightExprHolder.isOptional()) {
      // handle null == null
      cg.getEvalBlock()._if(compareLeftExprHolder.getIsSet().eq(JExpr.lit(0))
          .cand(compareRightExprHolder.getIsSet().eq(JExpr.lit(0))))
          ._then()
          ._return(JExpr.lit(0));
  
      // handle null == !null
      cg.getEvalBlock()._if(compareLeftExprHolder.getIsSet().eq(JExpr.lit(0))
          .cor(compareRightExprHolder.getIsSet().eq(JExpr.lit(0))))
          ._then()
          ._return(JExpr.lit(1));
    }

    // equality
    cg.getEvalBlock()._if(compareLeftExprHolder.getValue().eq(compareRightExprHolder.getValue()))
                     ._then()
                       ._return(JExpr.lit(0));
    // less than
    cg.getEvalBlock()._if(compareLeftExprHolder.getValue().lt(compareRightExprHolder.getValue()))
                     ._then()
                       ._return(JExpr.lit(-1));
    // greater than
    cg.getEvalBlock()._return(JExpr.lit(1));


    // generate compareNextLeftKey()
    ////////////////////////////////
    cg.setMappingSet(JoinInnerSignature.COMPARE_LEFT_MAPPING);
    cg.getSetupBlock().assign(JExpr._this().ref(incomingRecordBatch), JExpr._this().ref(incomingLeftRecordBatch));

    // int nextLeftIndex = leftIndex + 1;
    cg.getEvalBlock().decl(JType.parse(cg.getModel(), "int"), "nextLeftIndex", JExpr.direct("leftIndex").plus(JExpr.lit(1)));

    // check if the next key is in this batch
    cg.getEvalBlock()._if(joinStatus.invoke("isNextLeftPositionInCurrentBatch").eq(JExpr.lit(false)))
                     ._then()
                       ._return(JExpr.lit(-1));

    // generate VV read expressions
    CodeGenerator.HoldingContainer compareThisLeftExprHolder = cg.addExpr(materializedLeftExpr, false);
    cg.setMappingSet(JoinInnerSignature.COMPARE_NEXT_LEFT_MAPPING); // change mapping from 'leftIndex' to 'nextLeftIndex'
    CodeGenerator.HoldingContainer compareNextLeftExprHolder = cg.addExpr(materializedLeftExpr, false);

    if (compareThisLeftExprHolder.isOptional() || compareNextLeftExprHolder.isOptional()) {
      // handle null == null
      cg.getEvalBlock()._if(compareThisLeftExprHolder.getIsSet().eq(JExpr.lit(0))
                            .cand(compareNextLeftExprHolder.getIsSet().eq(JExpr.lit(0))))
                       ._then()
                         ._return(JExpr.lit(0));
  
      // handle null == !null
      cg.getEvalBlock()._if(compareThisLeftExprHolder.getIsSet().eq(JExpr.lit(0))
                            .cor(compareNextLeftExprHolder.getIsSet().eq(JExpr.lit(0))))
                       ._then()
                         ._return(JExpr.lit(1));
    }

    // check value equality
    cg.getEvalBlock()._if(compareThisLeftExprHolder.getValue().eq(compareNextLeftExprHolder.getValue()))
                     ._then()
                       ._return(JExpr.lit(0));

    // no match if reached
    cg.getEvalBlock()._return(JExpr.lit(1));


    // generate copyLeft()
    //////////////////////
    cg.setMappingSet(JoinInnerSignature.COPY_LEFT_MAPPING);
    int vectorId = 0;
    for (VectorWrapper vw : left) {
      JVar vvIn = cg.declareVectorValueSetupAndMember("incomingLeft",
                                                      new TypedFieldId(vw.getField().getType(), vectorId));
      JVar vvOut = cg.declareVectorValueSetupAndMember("outgoing",
                                                       new TypedFieldId(vw.getField().getType(),vectorId), true);
      // todo: check for room in vvOut
      cg.getEvalBlock().add(vvOut.invoke("copyFrom")
                                   .arg(JoinInnerSignature.COPY_LEFT_MAPPING.getValueReadIndex())
                                   .arg(JoinInnerSignature.COPY_LEFT_MAPPING.getValueWriteIndex())
                                   .arg(vvIn));
      ++vectorId;
    }
    cg.getEvalBlock()._return(JExpr.lit(true));

    // generate copyRight()
    ///////////////////////
    cg.setMappingSet(JoinInnerSignature.COPY_RIGHT_MAPPING);

    int rightVectorBase = vectorId;
    for (VectorWrapper vw : right) {
      JVar vvIn = cg.declareVectorValueSetupAndMember("incomingRight",
                                                      new TypedFieldId(vw.getField().getType(), vectorId - rightVectorBase));
      JVar vvOut = cg.declareVectorValueSetupAndMember("outgoing",
                                                       new TypedFieldId(vw.getField().getType(),vectorId),
                                                       true);
      cg.getEvalBlock().add(vvOut.invoke("copyFrom")
          .arg(JoinInnerSignature.COPY_RIGHT_MAPPING.getValueReadIndex())
          .arg(JoinInnerSignature.COPY_RIGHT_MAPPING.getValueWriteIndex())
          .arg(vvIn));
      ++vectorId;
    }
    cg.getEvalBlock()._return(JExpr.lit(true));

    JoinWorker w = context.getImplementationClass(cg);
    w.setupJoin(context, status, this.container);
    return w;
  }

  private void allocateBatch() {
    // allocate new batch space.
    container.clear();

    // add fields from both batches
    for (VectorWrapper w : left) {
      ValueVector outgoingVector = TypeHelper.getNewVector(w.getField(), context.getAllocator());
      getAllocator(w.getValueVector(), outgoingVector).alloc(left.getRecordCount() * 4);
      container.add(outgoingVector);
    }

    for (VectorWrapper w : right) {
      ValueVector outgoingVector = TypeHelper.getNewVector(w.getField(), context.getAllocator());
      getAllocator(w.getValueVector(), outgoingVector).alloc(right.getRecordCount() * 4);
      container.add(outgoingVector);
    }

    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    logger.debug("Built joined schema: {}", container.getSchema());
  }

  private VectorAllocator getAllocator(ValueVector in, ValueVector outgoing){
    if(outgoing instanceof FixedWidthVector){
      return new FixedVectorAllocator((FixedWidthVector) outgoing);
    }else if(outgoing instanceof VariableWidthVector && in instanceof VariableWidthVector){
      return new VariableVectorAllocator( (VariableWidthVector) in, (VariableWidthVector) outgoing);
    }else{
      throw new UnsupportedOperationException();
    }
  }

  private class FixedVectorAllocator implements VectorAllocator{
    FixedWidthVector out;

    public FixedVectorAllocator(FixedWidthVector out) {
      super();
      this.out = out;
    }

    public void alloc(int recordCount){
      out.allocateNew(recordCount);
      out.getMutator().setValueCount(recordCount);
    }
  }

  private class VariableVectorAllocator implements VectorAllocator{
    VariableWidthVector in;
    VariableWidthVector out;

    public VariableVectorAllocator(VariableWidthVector in, VariableWidthVector out) {
      super();
      this.in = in;
      this.out = out;
    }

    public void alloc(int recordCount){
      out.allocateNew(in.getByteCapacity(), recordCount);
      out.getMutator().setValueCount(recordCount);
    }
  }

  public interface VectorAllocator{
    public void alloc(int recordCount);
  }

}
