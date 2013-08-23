package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.NullableIntVector;

/**
 * This join template uses a merge join to combine two ordered streams into a single larger batch.  When joining
 * single values on each side, the values can be copied to the outgoing batch immediately.  The outgoing record batch
 * should be sent as needed (e.g. schema change or outgoing batch full).  When joining multiple values on one or
 * both sides, two passes over the vectors will be made; one to construct the selection vector, and another to
 * generate the outgoing batches once the duplicate value is no longer encountered.
 *
 * Given two tables ordered by 'col1':
 *
 *        t1                t2
 *  ---------------   ---------------
 *  | key | col2 |    | key | col2 |
 *  ---------------   ---------------
 *  |  1  | 'ab' |    |  1  | 'AB' |
 *  |  2  | 'cd' |    |  2  | 'CD' |
 *  |  2  | 'ef' |    |  4  | 'EF' |
 *  |  4  | 'gh' |    |  4  | 'GH' |
 *  |  4  | 'ij' |    |  5  | 'IJ' |
 *  ---------------   ---------------
 *
 * 'SELECT * FROM t1 INNER JOIN t2 on (t1.key == t2.key)' should generate the following:
 *
 * ---------------------------------
 * | t1.key | t2.key | col1 | col2 |
 * ---------------------------------
 * |   1    |   1    | 'ab' | 'AB' |
 * |   2    |   2    | 'cd' | 'CD' |
 * |   2    |   2    | 'ef' | 'CD' |
 * |   4    |   4    | 'gh' | 'EF' |
 * |   4    |   4    | 'gh' | 'GH' |
 * |   4    |   4    | 'ij' | 'EF' |
 * |   4    |   4    | 'ij' | 'GH' |
 * ---------------------------------
 *
 * In the simple match case, only one row from each table matches.  Additional cases should be considered:
 *   - a left join key matches multiple right join keys
 *   - duplicate keys which may span multiple record batches (on the left and/or right side)
 *   - one or both incoming record batches change schemas
 *
 * In the case where a left join key matches multiple right join keys:
 *   - add a reference to all of the right table's matching values to the SV4.
 *
 * A RecordBatchData object should be used to hold onto all batches which have not been sent.
 *
 * JoinStatus:
 *   - all state related to the join operation is stored in the JoinStatus object.
 *   - this is required since code may be regenerated before completion of an outgoing record batch.
 */
public class JoinTemplate implements JoinWorker {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinTemplate.class);

  private Copier copier;

  @Override
  public void setupJoin(JoinStatus status, VectorContainer outgoing){
    setup(status.getLeftBatch(), status.getRightBatch(), outgoing);
  }

  /**
   * Copy rows from the input record batches until the output record batch is full
   * @param status  State of the join operation (persists across multiple record batches/schema changes)
   */
  public final void doJoin(final JoinStatus status) {
    while (true) {
      // for each record

      // validate position and advance to the next record batch if necessary
      if (!status.isLeftPositionAllowed()) return;
      if (!status.isRightPositionAllowed()) return;

      int comparison = compare(status.getLeftPosition(), status.getRightPosition());
      switch (comparison) {

      case -1:
        // left key < right key
        copier.copyLeftRecord(status.getLeftPosition(), status.fetchAndIncOutputPos());
        status.advanceLeft();
        continue;

      case 0:
        // left key == right key
        if (!status.isLeftRepeating() &&
            status.isNextLeftPositionInCurrentBatch() &&
            compareNextLeftKey(status.getLeftPosition()) == 0) {
          // subsequent record(s) in the left batch have the same key
          // TODO: leftHasDups = true, if next left key matches but is in a new batch
//          status.notifyLeftRepeating();
        }

        int initialRightPosition = status.getRightPosition();
        do {
          logger.debug("Positions:  left: {}, right: {}, out: {}", status.getLeftPosition(), status.getRightPosition(), status.getOutPosition());
          // copy all equal right keys to the output record batch
          if (!copy(status.getLeftPosition(), status.getRightPosition(), status.fetchAndIncOutputPos()))
            return;

          // If the left key has duplicates and we're about to cross batch boundaries, queue the
          // right table's record batch before calling next.  These records will need to be copied
          // again for each duplicate left key.
          if (status.isLeftRepeating() && !status.isNextRightPositionInCurrentBatch()) {
            // last record in right batch is a duplicate, and at the end of the batch
            status.outputBatch.addRightToBatchBuilder();
          }
          status.advanceRight();
        } while (status.isRightPositionAllowed() && compare(status.getLeftPosition(), status.getRightPosition()) == 0);
        if (status.getRightPosition() > initialRightPosition + 1)
          // more than one matching result from right table
          status.setRightPosition(initialRightPosition);
        status.advanceLeft();

        if (status.isLeftRepeating() && compareNextLeftKey(status.getLeftPosition()) != 0) {
          // left no longer has duplicates.  switch back to incoming batch mode
//          status.setDefaultAdvanceMode();
//          status.notifyLeftStoppedRepeating();
        } else if (status.isLeftRepeating()) {
          // left is going to repeat; use sv4 for right batch
//          status.setRepeatedAdvanceMode();
        }          
        continue;

      case 1:
        // left key > right key
        status.advanceRight();
        continue;

      default:
        throw new IllegalStateException();
      }
    }
  }

  //
  // Stub Implementations
  //

  /**
   * Copy the data to the new record batch (if it fits).
   *
   * @param leftPosition  position of batch (lower 16 bits) and record (upper 16 bits) in left SV4
   * @param rightPosition position of batch (lower 16 bits) and record (upper 16 bits) in right SV4
   * @param outputPosition position of the output record batch
   * @return Whether or not the data was copied.
   */
  protected boolean copy(int leftPosition, int rightPosition, int outputPosition) {
    return copier.copyRecord(leftPosition, rightPosition, outputPosition);
  }

  protected boolean copyLeft(int leftPosition, int outputPosition) {
    return copier.copyLeftRecord(leftPosition, outputPosition);
  }
  
  /**
   * Compare the values of the left and right join key to determine whether the left is less than, greater than
   * or equal to the right.
   *
   * @param leftPosition
   * @param rightPosition
   * @return  0 if both keys are equal
   *         -1 if left is < right
   *          1 if left is > right
   */
  protected int compare(int leftPosition, int rightPosition) {
    return copier.compare(leftPosition, rightPosition);
  }

  protected int compareNextLeftKey(int position) {
    return copier.compareNextLeftKey(position);
  }

  public void setup(RecordBatch left, RecordBatch right, VectorContainer outgoing) {
    copier = new Copier(left, right, outgoing);
  }

  private class Copier {
    private RecordBatch left;
    private RecordBatch right;
    private VectorContainer out;
    private VectorWrapper leftKeyVV;
    private VectorWrapper rightKeyVV;

    public Copier(RecordBatch left,
                  RecordBatch right,
                  VectorContainer out) {

      this.left = left;
      this.right = right;
      this.out = out;

      // TODO: VV position
      leftKeyVV = left.getValueAccessorById(0, left.iterator().next().getVectorClass());
      rightKeyVV = right.getValueAccessorById(0, right.iterator().next().getVectorClass());
    }

    public boolean copyRecord(int leftPosition, int rightPosition, int outPosition) {

      logger.debug("Positions:  left: {}, right: {}, out: {}", leftPosition, rightPosition, outPosition);
      // copy values from left batch
      int outVectorId = 0;
      for (VectorWrapper w : left) {
        if (w.getValueVector().getValueCapacity() < leftPosition) {
          logger.error("Output has grown too large");
          return false;
        }

        ((NullableIntVector) out.getVectorAccessor(outVectorId, w.getValueVector()
            .getField()
            .getValueClass())
            .getValueVector())
            .copyFrom(outPosition, leftPosition, (NullableIntVector) w.getValueVector());
        ++outVectorId;
      }

      // copy values from the right batch
      for (VectorWrapper w : right) {
        if (w.getValueVector().getValueCapacity() < rightPosition) {
          logger.error("Output has grown too large");
          return false;
        }

        ((NullableIntVector) out.getVectorAccessor(outVectorId, w.getValueVector()
            .getField()
            .getValueClass())
            .getValueVector())
            .copyFrom(outPosition, rightPosition, (NullableIntVector) w.getValueVector());
        ++outVectorId;
      }

      return true;
    }

    public boolean copyLeftRecord(int leftPosition, int outPosition) {
      // copy from left batch
      int outVectorId = 0;
      for (VectorWrapper w : left) {
        if (w.getValueVector().getValueCapacity() < leftPosition) {
          logger.error("Output has grown too large");
          return false;
        }

        ((NullableIntVector) out.getVectorAccessor(outVectorId, w.getValueVector()
            .getField()
            .getValueClass())
            .getValueVector())
            .copyFrom(outPosition, leftPosition, (NullableIntVector) w.getValueVector());
        ++outVectorId;
      }

      return true;
    }

    /**
     * Compare the current left and right keys.  Assumes null is less than all values.
     */
    public int compare(int leftPosition, int rightPosition) {

      // handle null values
      if (((NullableIntVector) leftKeyVV.getValueVector()).getAccessor().isNull(leftPosition)) {
        if (((NullableIntVector) rightKeyVV.getValueVector()).getAccessor().isNull(rightPosition)) {
          // null == null
          return 0;
        } else {
          // null == non-null
          return -1;
        }
      } else if (((NullableIntVector) rightKeyVV.getValueVector()).getAccessor().isNull(rightPosition)) {
        // non-null == null
        return 1;
      }

      logger.debug("[compare] left: {} vs. right: {}",
                          ((NullableIntVector) leftKeyVV.getValueVector()).getAccessor().get(leftPosition),
                          ((NullableIntVector) rightKeyVV.getValueVector()).getAccessor().get(rightPosition));

      if (((NullableIntVector) leftKeyVV.getValueVector()).getAccessor().get(leftPosition) >
          ((NullableIntVector) rightKeyVV.getValueVector()).getAccessor().get(rightPosition))
        return 1;
      else if (((NullableIntVector) leftKeyVV.getValueVector()).getAccessor().get(leftPosition) <
          ((NullableIntVector) rightKeyVV.getValueVector()).getAccessor().get(rightPosition))
        return -1;
      return 0;
    }

    /**
     * Compare the current and next key in the left table.  Assumes null is less than all values.
     * TODO: handle next left position being in another batch
     */
    public int compareNextLeftKey(int position) {

      // handle null values
      if (((NullableIntVector) leftKeyVV.getValueVector()).getAccessor().isNull(position) &&
          leftKeyVV.getValueVector().getAccessor().getValueCount() > position + 1 &&
          ((NullableIntVector) leftKeyVV.getValueVector()).getAccessor().isNull(position+1))
        return 0;
      else if (!((NullableIntVector) leftKeyVV.getValueVector()).getAccessor().isNull(position) &&
               leftKeyVV.getValueVector().getAccessor().getValueCount() > position + 1 &&
               ((NullableIntVector) leftKeyVV.getValueVector()).getAccessor().isNull(position+1))
        return -1;
      else if (((NullableIntVector) leftKeyVV.getValueVector()).getAccessor().isNull(position))
        return 1;
      
      logger.debug("[compare] current left key: {} vs. next left key: {}",
                   ((NullableIntVector) leftKeyVV.getValueVector()).getAccessor().get(position),
                   (leftKeyVV.getValueVector().getAccessor().getValueCount() > position + 1 ?
                       ((NullableIntVector) leftKeyVV.getValueVector()).getAccessor().get(position + 1) : "<EOB>"));

      // handle non-null cases
      if (leftKeyVV.getValueVector().getAccessor().getValueCount() > position + 1 &&
          ((NullableIntVector) leftKeyVV.getValueVector()).getAccessor().get(position) ==
          ((NullableIntVector) leftKeyVV.getValueVector()).getAccessor().get(position+1))
        return 0;
      if (leftKeyVV.getValueVector().getAccessor().getValueCount() > position + 1 &&
          ((NullableIntVector) leftKeyVV.getValueVector()).getAccessor().get(position) >
          ((NullableIntVector) leftKeyVV.getValueVector()).getAccessor().get(position+1))
        return 1;
      return -1;
    }

  }

// TODO: code generation
//  /**
//   * Copy the data to the new record batch (if it fits).
//   *
//   * @param leftPosition  position of batch (lower 16 bits) and record (upper 16 bits) in left SV4
//   * @param rightPosition position of batch (lower 16 bits) and record (upper 16 bits) in right SV4
//   * @param outputPosition position of the output record batch
//   * @return Whether or not the data was copied.
//   */
//  protected abstract boolean copy(int leftPosition, int rightPosition, int outputPosition);
//
//  /**
//   * Compare the values of the left and right join key to determine whether the left is less than, greater than
//   * or equal to the right.
//   *
//   * @param leftPosition
//   * @param rightPosition
//   * @return  0 if both keys are equal
//   *         -1 if left is < right
//   *          1 if left is > right
//   */
//  protected abstract int compare(int leftPosition, int rightPosition);
//  protected abstract int compareNextLeftKey(int position);
//  public abstract void setup(RecordBatch left, RecordBatch right, RecordBatch outgoing);

}
