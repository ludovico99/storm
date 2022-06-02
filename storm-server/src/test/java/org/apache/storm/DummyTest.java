package org.apache.storm;

import org.apache.storm.healthcheck.HealthChecker;
import org.junit.Assert;
import org.junit.Test;

public class DummyTest {


    @Test
    public void dummyMethod (){
        HealthChecker ht = new HealthChecker();
        Assert.assertTrue(true);
    }
}
