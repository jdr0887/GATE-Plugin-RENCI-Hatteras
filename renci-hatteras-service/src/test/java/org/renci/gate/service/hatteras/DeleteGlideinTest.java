package org.renci.gate.service.hatteras;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Executors;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;
import org.renci.jlrm.JobStatusInfo;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.renci.jlrm.slurm.SLURMJobStatusType;
import org.renci.jlrm.slurm.ssh.SLURMSSHKillCallable;
import org.renci.jlrm.slurm.ssh.SLURMSSHLookupStatusCallable;

public class DeleteGlideinTest {

    @Test
    public void test() throws Exception {
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

        SLURMSSHLookupStatusCallable callable = new SLURMSSHLookupStatusCallable(site);
        Set<JobStatusInfo> jobs = Executors.newSingleThreadExecutor().submit(callable).get();

        String jobName = String.format("glidein-%s", site.getName().toLowerCase());
        if (CollectionUtils.isNotEmpty(jobs)) {
            for (JobStatusInfo info : jobs) {
                if (!info.getJobName().equals(jobName)) {
                    continue;
                }
                if (!info.getStatus().equals(SLURMJobStatusType.RUNNING.toString())) {
                    continue;
                }
                SLURMSSHKillCallable killCallable = new SLURMSSHKillCallable(site, info.getJobId());
                Executors.newSingleThreadExecutor().submit(killCallable).get();
                break;
            }
        }
        
    }
}
