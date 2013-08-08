package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.exec.physical.impl.join.JoinWorker.JoinOutcome;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;

import com.google.common.base.Preconditions;

/**
 * The status of the current join.  Maintained outside the individually compiled join templates so that we can carry status across multiple schemas.
 */
public final class JoinStatus {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinStatus.class);

  public int leftPosition;
  private final RecordBatch left;
  private IterOutcome lastLeft;

  public int rightPosition;
  private final RecordBatch right;
  private IterOutcome lastRight;
  
  public int outputPosition;
  
  private boolean initialSet = false;
  
  public JoinStatus(RecordBatch left, RecordBatch right) {
    super();
    this.left = left;
    this.right = right;
  }

  public final void ensureInitial(){
    if(!initialSet){
      this.lastLeft = left.next();
      this.lastRight = right.next();
      initialSet = true;
    }
  }
  
  public final void advanceLeft(){
    leftPosition++;
  }
  
  public final void advanceRight(){
    rightPosition++;
  }
  
  public final void advanceAll(){
    advanceLeft();
    advanceRight();
    outputPosition++;
  }
  
  public final boolean isLeftPositionAllowed(){
    if(leftPosition >= left.getRecordCount()){
      leftPosition = 0;
      lastLeft = left.next();
      return lastLeft == IterOutcome.OK;
    }else{
      lastLeft = IterOutcome.OK;
      return true;
    }
  }
  
  public final boolean isRightPositionAllowed(){
    if(rightPosition >= right.getRecordCount()){
      rightPosition = 0;
      lastRight = right.next();
      return lastRight == IterOutcome.OK;
    }else{
      lastRight = IterOutcome.OK;
      return true;
    }
    
  }

  public JoinOutcome getOutcome(){
    if(lastLeft == IterOutcome.OK && lastRight == IterOutcome.OK){
      return JoinOutcome.BATCH_RETURNED;
    }else if(eitherMatches(IterOutcome.OK_NEW_SCHEMA)){
      return JoinOutcome.SCHEMA_CHANGED;
    }else if(eitherMatches(IterOutcome.NOT_YET)){
      return JoinOutcome.WAITING;
    }else if(eitherMatches(IterOutcome.NONE)){
      return JoinOutcome.NO_MORE_DATA;
    }else{
      return JoinOutcome.FAILURE;
    }
  }
  
  private boolean eitherMatches(IterOutcome outcome){
    return lastLeft == outcome || lastRight == outcome;
  }
  
}
