package org.apache.storm;


import org.apache.storm.topology.OutputFieldsGetter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DummyTest2 {

    @Test
    public void dummyMethod (){
        OutputFieldsGetter ot = new OutputFieldsGetter();
        assertEquals(ot.getClass(), OutputFieldsGetter.class);
    }
}
