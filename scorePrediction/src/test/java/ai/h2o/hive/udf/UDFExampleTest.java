package ai.h2o.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.junit.Test;
import junit.framework.Assert;

public class UDFExampleTest {
  @Test public void testUDFReturnsCorrectValues() throws HiveException {
   
	  // set up the models we need
    ScoreDataMainUDF example = new ScoreDataMainUDF();
    //From the test data set: "AGEP", "COW", "SCHL", "MAR", "INDP", "RELP", "RAC1P", "SEX", "WKHP", "POBP", "LOG_CAPGAIN", "LOG_CAPLOSS"
    ObjectInspector C1 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    ObjectInspector C2 = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    ObjectInspector C3 = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    ObjectInspector C4 = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    ObjectInspector C5 = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    ObjectInspector C6 = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    ObjectInspector C7 = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    ObjectInspector C8 = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    ObjectInspector C9 = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
    ObjectInspector C10 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    JavaDoubleObjectInspector resultInspector = (JavaDoubleObjectInspector) example.initialize(new ObjectInspector[]{C1, C2, C3,
    		C4, C5, C6, C7, C8, C9, C10});
    // test our results
    // Data from first line of test file: 
    Object result =  example.evaluate(new DeferredObject[]{new DeferredJavaObject(0), new DeferredJavaObject(79.00524687669414), // AGEP, COW 
            new DeferredJavaObject(1), new DeferredJavaObject(62.34107756773965), new DeferredJavaObject(94.72604293120712), // SCHL, MAR, INDP
            new DeferredJavaObject(-3.1683586213359094), new DeferredJavaObject(37.19742854068111),new DeferredJavaObject(-46.56359552425913), // RELP, RAC1P, SEX
            new DeferredJavaObject(-62.62769638188415), new DeferredJavaObject(-61)}); // LOG_CAPLOSS
    double tolerance = 1e-4;
    
    if(result instanceof double[]) {
        double[] doublVals = (double[]) result;
        System.out.println(result.getClass().getTypeName());
    	Assert.assertEquals(1.0, doublVals[0]);
    }else if(result instanceof Double[]) {
    	Double[] doublVals = (Double[]) result;
            System.out.println(result.getClass().getTypeName());
        	Assert.assertEquals(1.0, ((Double)doublVals[0]).doubleValue());
        	Assert.assertEquals(0.4818177, doublVals[1],tolerance);
        	Assert.assertEquals(0.51818, doublVals[2],tolerance);
    }else if(result instanceof Double) {
    	Assert.assertEquals(1.0, ((Double)result).doubleValue(),tolerance);
    }
    
    
    
    // Wrong number of arguments

    try {
      example.evaluate(new DeferredObject[]{new DeferredJavaObject("0"), new DeferredJavaObject("21")});
      Assert.fail();
    } catch (UDFArgumentException expected) { Assert.assertTrue(true);}
    // Arguments are null
    Object result3 = example.evaluate(new DeferredObject[]{new DeferredJavaObject(null), new DeferredJavaObject(null), // AGEP, COW 
            new DeferredJavaObject(null), new DeferredJavaObject(null),new DeferredJavaObject(null), // SCHL, MAR, INDP 
            new DeferredJavaObject(null), new DeferredJavaObject(null), new DeferredJavaObject(null), //RELP, RAC1P, SEX
            new DeferredJavaObject(null), new DeferredJavaObject(null)}); // LOG_CAPLOSS
    Assert.assertNull(result3);
    
  }
}
