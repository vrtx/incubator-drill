package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;

/**
 * This join template uses a merge join to combine two ordered streams into a single larger batch.  When joining
 * single values on each side, the values can be copied to the output batch immediately.  The outgoing record batch
 * should be sent as needed (e.g. schema change or output batch full).  When joining multiple values on one or
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
 * ---------------------------------
 *
 * In the simple case, there is a single record batch and no schema change.  Additional logic is required to handle:
 *   - one or both table's batch may change schemas at any time
 *   - duplicate keys may span multiple record batches (on the left and/or right side)
 *
 * In the case where a match is encountered, and subsequent duplicate keys are encountered on the right table:
 *   - add a reference to all of the right table's matching values to the SV4
 *   - a RecordBatchData object should be used to hold onto all batches which have not been sent.
 */
public abstract class JoinTemplate implements JoinWorker {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinTemplate.class);

  @Override
  public void setupJoin(JoinStatus status, VectorContainer outgoing){
    
  }

  public final void doJoin(final JoinStatus status) {


    while (true) {
      // for each record

      if (!status.isLeftPositionAllowed()) return;
      if (!status.isRightPositionAllowed()) return;

      // compare join fields
      int comparison = compare(status.leftPosition, status.rightPosition);
      switch (comparison) {
      case -1:
        status.advanceLeft();
        if (!status.isLeftPositionAllowed()) return;
        continue;
      case 0:

        if (copy(status.leftPosition, status.rightPosition, status.outputPosition)) {
          status.advanceAll(); // TODO: can't advance unless rhs is exceeded
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
   * @param leftPosition  position of batch in left SV4 (upper 16 bits)
   * @param rightPosition position of record in left SV4 (lower 16 bits)
   * @param outputPosition
   * @return Whether or not the data was copied.
   */
  protected abstract boolean copy(int leftPosition, int rightPosition, int outputPosition);
  
  /**
   * Compare the values of the two sides of the join and determine whether or not they are equal.
   * @param leftPosition
   * @param rightPosition
   * @return  0 if the two values are equal
   *         -1 if left is less than right
   *          1 if left is greater than right
   */
  protected abstract int compare(int leftPosition, int rightPosition);
  public abstract void setup(RecordBatch left, RecordBatch right, RecordBatch outgoing);
}
