package org.apache.storm;

import org.apache.storm.healthcheck.HealthChecker;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DummyTest {


    @Test
    public void dummyMethod (){
        HealthChecker ht = new HealthChecker();
        assertEquals(ht.getClass(), HealthChecker.class);
    }
}
