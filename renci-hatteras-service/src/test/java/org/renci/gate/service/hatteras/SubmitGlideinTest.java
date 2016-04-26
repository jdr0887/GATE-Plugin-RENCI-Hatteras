package org.renci.gate.service.hatteras;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;

public class SubmitGlideinTest {

    @Test
    public void test() {

        Queue queue = new Queue();
        queue.setMaxPending(4);
        queue.setMaxRunning(20);
        queue.setName("batch");
        queue.setWeight(1.0);
        queue.setRunTime(5760L);
        queue.setNumberOfProcessors(16);

        Site site = new Site();
        site.setName("Hatteras");
        site.setProject("RENCI");
        site.setSubmitHost("ht0.renci.org");
        site.setUsername("mapseq");
        site.setQueueList(Arrays.asList(queue));

        try {
            HatterasSubmitCondorGlideinCallable callable = new HatterasSubmitCondorGlideinCallable();
            callable.setCollectorHost("mps.renci.org");
            callable.setUsername("mapseq");
            callable.setSite(site);
            callable.setJobName(String.format("glidein-%s", site.getName().toLowerCase()));
            callable.setQueue(queue);
            callable.setRequiredMemory("90000");
            callable.setHostAllowRead("*.renci.org");
            callable.setHostAllowWrite("*.renci.org");
            Executors.newSingleThreadExecutor().submit(callable).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }
}
