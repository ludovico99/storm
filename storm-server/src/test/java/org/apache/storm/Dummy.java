package org.apache.storm;

import org.apache.storm.healthcheck.HealthChecker;
import org.junit.Assert;
import org.junit.Test;

public class Dummy {


    @Test
    public void dummyMethod (){
        HealthChecker ht = new HealthChecker();
        Assert.assertEquals(ht.getClass(), HealthChecker.class);
    }
}
