package org.apache.drill.exec.physical.impl.join;

import java.io.IOException;

import com.sun.codemodel.*;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MergeJoinPOP;
import org.apache.drill.exec.physical.impl.join.JoinWorker.JoinOutcome;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.TypeHelper;

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
      
      boolean first = false;
      if(worker == null){
        try {
          logger.debug("Creating New Worker");
          this.worker = getNewWorker();
          first = true;
        } catch (ClassTransformationException | IOException e) {
          context.fail(new SchemaChangeException(e));
          kill();
          return IterOutcome.STOP;
        }
      }

      // if the previous outcome was a change in schema or we sent a batch, we have to set up a new batch.
      if (status.getOutcome() == JoinOutcome.BATCH_RETURNED ||
          status.getOutcome() == JoinOutcome.SCHEMA_CHANGED)
        allocateBatch();

      // reset the output position to zero after our parent iterates this RecordBatch
      if (status.getOutcome() == JoinOutcome.BATCH_RETURNED ||
          status.getOutcome() == JoinOutcome.SCHEMA_CHANGED ||
          status.getOutcome() == JoinOutcome.NO_MORE_DATA)
       status.resetOutputPos();

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

  private JoinWorker getNewWorker() throws ClassTransformationException, IOException{

    CodeGenerator<JoinWorker> cg = new CodeGenerator<JoinWorker>(JoinWorker.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    final ErrorCollector collector = new ErrorCollectorImpl();
    final LogicalExpression leftFieldExpr = condition.getLeft();
    final LogicalExpression rightFieldExpr = condition.getRight();

    final LogicalExpression materializedLeftExpr = ExpressionTreeMaterializer.materialize(leftFieldExpr, left, collector);
    if (collector.hasErrors())
      throw new ClassTransformationException(String.format(
          "Failure while trying to materialize incoming left schema.  Errors:\n %s.", collector.toErrorString()));

    final LogicalExpression materializedRightExpr = ExpressionTreeMaterializer.materialize(rightFieldExpr, right, collector);
    if (collector.hasErrors())
      throw new ClassTransformationException(String.format(
          "Failure while trying to materialize incoming right schema.  Errors:\n %s.", collector.toErrorString()));

    // generate evaluate expression to determine the hash
    CodeGenerator.HoldingContainer leftExprHolder = cg.addExpr(materializedLeftExpr);
    CodeGenerator.HoldingContainer rightExprHolder = cg.addExpr(materializedRightExpr);

    JClass schemaPathClass = cg.getModel().ref(SchemaPath.class);
    JVar leftSchemaPath = cg.clazz.field(JMod.NONE,
        schemaPathClass,
        "leftSchemaPath");
    cg.getSetupBlock().assign(leftSchemaPath, leftExprHolder.getHolder());

    JVar rightSchemaPath = cg.clazz.field(JMod.NONE,
        schemaPathClass,
        "rightSchemaPath");
    cg.getSetupBlock().assign(rightSchemaPath, rightExprHolder.getValue());
    
    
    if (status.rightSourceMode == JoinStatus.RightSourceMode.INCOMING_BATCHES) {
      // generate direct copier.
      // for each 
    } else {
      // generate copier which deref's SV4
    }

    // generate comparator.
    
    // generate compareNextLeftKey.

    JoinWorker w = context.getImplementationClass(cg);
    w.setupJoin(status, this.container);
    return w;
  }

  private void allocateBatch() {
    // allocate new batch space.
    container.clear();

    // add fields from both batches
    for (VectorWrapper w : left) {
      logger.debug("Adding left join vector: {}", w.getValueVector().getField());
      NullableIntVector v = (NullableIntVector)TypeHelper.getNewVector(w.getField(), context.getAllocator());
      v.allocateNew(w.getValueVector().getValueCapacity());
      container.add(v);
    }

    for (VectorWrapper w : right) {
      // todo: handle duplicate field names
      logger.debug("Adding right join vector: {}", w.getValueVector().getField());
      NullableIntVector v = (NullableIntVector)TypeHelper.getNewVector(w.getField(), context.getAllocator());
      v.allocateNew(w.getValueVector().getValueCapacity());
      container.add(v);
    }

    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    logger.debug("Built joined schema: {}", container.getSchema());

  }
}
