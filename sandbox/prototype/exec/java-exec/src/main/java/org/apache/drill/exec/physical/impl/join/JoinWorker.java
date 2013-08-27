package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.VectorContainer;


public interface JoinWorker {
  
  public static enum JoinOutcome {
    NO_MORE_DATA, BATCH_RETURNED, SCHEMA_CHANGED, WAITING, FAILURE;
  }

  public void setupJoin(FragmentContext context, JoinStatus status, VectorContainer outgoing);
  public void doJoin(JoinStatus status);
  
  public static TemplateClassDefinition<JoinWorker> TEMPLATE_DEFINITION = new TemplateClassDefinition<>(
      JoinWorker.class,
      "org.apache.drill.exec.physical.impl.join.JoinTemplate",
      JoinEvaluator.class,
      JoinInnerSignature.class);
  
}
