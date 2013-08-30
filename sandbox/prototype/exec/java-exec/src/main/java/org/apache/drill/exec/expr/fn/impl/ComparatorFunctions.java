package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.common.expression.ArgumentValidators;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.BigIntHolder;
import org.apache.drill.exec.vector.IntHolder;
import org.apache.drill.exec.vector.VarBinaryHolder;

public class ComparatorFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ComparatorFunctions.class);
  
  @FunctionTemplate(name = "compare_to", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class IntComparator implements DrillSimpleFunc {

      @Param IntHolder left;
      @Param IntHolder right;
      @Output IntHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        out.value = left.value < right.value ? -1 : ((left.value == right.value)? 0 : 1);
      }
  }
  
  @FunctionTemplate(name = "compare_to", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class VarBinaryComparator implements DrillSimpleFunc {

      @Param VarBinaryHolder left;
      @Param VarBinaryHolder right;
      @Output IntHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        boolean doLengthEval = true;
        int i =0;
        for (int l = left.start, r = right.start; l < left.end && r < right.end; l++, r++, i++) {
          byte leftByte = left.buffer.getByte(l);
          byte rightByte = right.buffer.getByte(r);
          if (leftByte != rightByte) {
            out.value = ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
            doLengthEval = false;
            break;
          }
        }
        if(doLengthEval){
          int l = (left.end - left.start) - (right.end - right.start);
          if(l > 0){
            out.value = 1;
          }else if(l == 0){
            out.value = 0;
          }else{
            out.value = -1;
          }
        }

      }
     
  }
  
  @FunctionTemplate(name = "compare_to", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Long implements DrillSimpleFunc {

      @Param BigIntHolder left;
      @Param BigIntHolder right;
      @Output IntHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        out.value = left.value < right.value ? -1 : ((left.value == right.value)? 0 : 1);
      }
  }
  public static final FunctionDefinition COMPARE_TO = FunctionDefinition.simple("compare_to", new ArgumentValidators.AllowedTypeList(2, Types.required(MinorType.INT)), new OutputTypeDeterminer.FixedType(Types.required(MinorType.INT)));
  public static class Provider implements CallProvider{

    @Override
    public FunctionDefinition[] getFunctionDefintions() {
      return new FunctionDefinition[]{
          COMPARE_TO
      };
    }
    
  }
}
