package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;


/**
 * This join template uses a merge join to combine two ordered streams into a single larger batch.  The 
 */
public abstract class JoinTemplate implements JoinWorker {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinTemplate.class);

  @Override
  public void setupJoin(JoinStatus status, VectorContainer outgoing){
    
  }


  
  public final void doJoin(final JoinStatus status) {

    while (true) {

      if (!status.isLeftPositionAllowed()) return;
      if (!status.isRightPositionAllowed()) return;

      int comparison = compare(status.leftPosition, status.rightPosition);
      switch (comparison) {
      case -1:
        status.advanceLeft();
        if (!status.isLeftPositionAllowed()) return;
        continue;
      case 0:

        if (copy(status.leftPosition, status.rightPosition, status.outputPosition)) {
          status.advanceAll();
          continue;
        } else {
          return;
        }

      case 1:
        status.advanceRight();
        if (!status.isRightPositionAllowed()) return;
        continue;

      default:
        throw new IllegalStateException();
      }

    }

  }

  
  /**
   * Copy the data to the new space if it fits.
   * 
   * @param leftPosition
   * @param rightPosition
   * @param outputPosition
   * @return Whether or not the data was copied.
   */
  protected abstract boolean copy(int leftPosition, int rightPosition, int outputPosition);
  
  /**
   * Compare the values of the two sides of the join and determine whether or not they are equal.
   * @param leftPosition
   * @param rightPosition
   * @return
   */
  protected abstract int compare(int leftPosition, int rightPosition);
  public abstract void setup(RecordBatch left, RecordBatch right, RecordBatch outgoing);
}
