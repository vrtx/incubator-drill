package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.base.AbstractExchange;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Receiver;
import org.apache.drill.exec.physical.base.Sender;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.List;

@JsonTypeName("single-merge-exchange")
public class SingleMergeExchange extends AbstractExchange {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleMergeExchange.class);

  private final LogicalExpression expr;

  // ephemeral for setup tasks
  private List<CoordinationProtos.DrillbitEndpoint> senderLocations;
  private CoordinationProtos.DrillbitEndpoint receiverLocation;

  @JsonCreator
  public SingleMergeExchange(@JsonProperty("child") PhysicalOperator child,
                              @JsonProperty("expr") LogicalExpression expr) {
    super(child);
    this.expr = expr;
  }

  @Override
  public int getMaxSendWidth() {
    return Integer.MAX_VALUE;
  }

  @Override
  protected void setupSenders(List<CoordinationProtos.DrillbitEndpoint> senderLocations) {
    this.senderLocations = senderLocations;
  }

  @Override
  protected void setupReceivers(List<CoordinationProtos.DrillbitEndpoint> receiverLocations)
      throws PhysicalOperatorSetupException {

    if (receiverLocations.size() != 1)
      throw new PhysicalOperatorSetupException("SingleMergeExchange only supports a single receiver endpoint");
    receiverLocation = receiverLocations.iterator().next();

  }

  @Override
  public Sender getSender(int minorFragmentId, PhysicalOperator child) {
    return new SingleSender(receiverMajorFragmentId, child, receiverLocation);
  }

  @Override
  public Receiver getReceiver(int minorFragmentId) {
    return new MergingReceiverPOP(senderMajorFragmentId, senderLocations, expr);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new SingleMergeExchange(child, expr);
  }

  @Override
  public boolean supportsSelectionVector() {
    return true;
  }

}