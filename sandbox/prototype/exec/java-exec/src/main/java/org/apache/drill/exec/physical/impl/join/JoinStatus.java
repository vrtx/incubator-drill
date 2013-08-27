package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.exec.physical.impl.join.JoinWorker.JoinOutcome;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.selection.SelectionVector4;

/**
 * The status of the current join.  Maintained outside the individually compiled join templates so that we can carry status across multiple schemas.
 */
public final class JoinStatus {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinStatus.class);

  public static enum RightSourceMode {
    INCOMING_BATCHES, QUEUED_BATCHES;
  }

  public final RecordBatch left;
  private int leftPosition;
  private IterOutcome lastLeft;

  public final RecordBatch right;
  private int rightPosition;
  private int svRightPosition;
  private IterOutcome lastRight;
  
  private int outputPosition;
  public RightSourceMode rightSourceMode = RightSourceMode.INCOMING_BATCHES;
  public MergeJoinBatch outputBatch;
  public SelectionVector4 sv4;

  private boolean initialSet = false;
  private boolean leftRepeating = false;
  
  public JoinStatus(RecordBatch left, RecordBatch right, MergeJoinBatch output) {
    super();
    this.left = left;
    this.right = right;
    this.outputBatch = output;
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
//    if (rightSourceMode == RightSourceMode.INCOMING_BATCHES)
      rightPosition++;
    // only advance the right position
    // advance through queued batches
  }

  public final int getLeftPosition() {
    return leftPosition;
  }

  public final int getRightPosition() {
    return rightPosition;
//    return (rightSourceMode == RightSourceMode.INCOMING_BATCHES) ? rightPosition : svRightPosition;
  }

  public final void setRightPosition(int pos) {
    rightPosition = pos;
  }


  public final int getOutPosition() {
    return outputPosition;
  }

  public final int fetchAndIncOutputPos() {
    return outputPosition++;
  }

  public final void resetOutputPos() {
    outputPosition = 0;
  }

  public final void notifyLeftRepeating() {
    leftRepeating = true;
//    outputBatch.resetBatchBuilder();
  }

  public final void notifyLeftStoppedRepeating() {
    leftRepeating = false;
  }

  public final boolean isLeftRepeating() {
    return leftRepeating;
  }

  public void setDefaultAdvanceMode() {
    rightSourceMode = RightSourceMode.INCOMING_BATCHES;
    rightPosition = 0;
  }

  public void setRepeatedAdvanceMode() {
    rightSourceMode = RightSourceMode.QUEUED_BATCHES;
    svRightPosition = 0;
  }

  /**
   * Check if the left record position can advance by one.
   * Side effect: advances to next left batch if current left batch size is exceeded.
   */
  public final boolean isLeftPositionAllowed(){
    if(!isLeftPositionInCurrentBatch()){
      leftPosition = 0;
      lastLeft = left.next();
      return lastLeft == IterOutcome.OK;
    }else{
      lastLeft = IterOutcome.OK;
      return true;
    }
  }

  /**
   * Check if the right record position can advance by one.
   * Side effect: advances to next right batch if current right batch size is exceeded
   */
  public final boolean isRightPositionAllowed(){
    if(!isRightPositionInCurrentBatch()){
      rightPosition = 0;
      lastRight = right.next();
      return lastRight == IterOutcome.OK;
    }else{
      lastRight = IterOutcome.OK;
      return true;
    }
    
  }

  /**
   * Check if the left record position can advance by one in the current batch.
   */
  public final boolean isLeftPositionInCurrentBatch() {
    return leftPosition < left.getRecordCount();
  }

  /**
   * Check if the right record position can advance by one in the current batch.
   */
  public final boolean isRightPositionInCurrentBatch() {
    return rightPosition < right.getRecordCount();
  }

  /**
   * Check if the next left record position can advance by one in the current batch.
   */
  public final boolean isNextLeftPositionInCurrentBatch() {
    return leftPosition + 1 < left.getRecordCount();
  }

  /**
   * Check if the next left record position can advance by one in the current batch.
   */
  public final boolean isNextRightPositionInCurrentBatch() {
    return rightPosition + 1 < right.getRecordCount();
  }

  public JoinOutcome getOutcome(){
    if (lastLeft == IterOutcome.OK && lastRight == IterOutcome.OK)
      return JoinOutcome.BATCH_RETURNED;
    if (eitherMatches(IterOutcome.OK_NEW_SCHEMA))
      return JoinOutcome.SCHEMA_CHANGED;
    if (eitherMatches(IterOutcome.NOT_YET))
      return JoinOutcome.WAITING;
    if (eitherMatches(IterOutcome.NONE))
      return JoinOutcome.NO_MORE_DATA;
    return JoinOutcome.FAILURE;
  }
  
  private boolean eitherMatches(IterOutcome outcome){
    return lastLeft == outcome || lastRight == outcome;
  }
  
}
