package org.renci.gate.service.hatteras;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.renci.gate.AbstractGATEService;
import org.renci.gate.GATEException;
import org.renci.gate.GlideinMetric;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.JobStatusInfo;
import org.renci.jlrm.Queue;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.renci.jlrm.slurm.SLURMJobStatusType;
import org.renci.jlrm.slurm.ssh.SLURMSSHKillCallable;
import org.renci.jlrm.slurm.ssh.SLURMSSHLookupStatusCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class HatterasGATEService extends AbstractGATEService {

    private static final Logger logger = LoggerFactory.getLogger(HatterasGATEService.class);

    private String numberOfProcessors;

    public HatterasGATEService() {
        super();
    }

    @Override
    public void init() throws GATEException {
        logger.debug("ENTERING init()");
        try {
            SLURMSSHLookupStatusCallable callable = new SLURMSSHLookupStatusCallable(getSite());
            Set<JobStatusInfo> jobs = Executors.newSingleThreadExecutor().submit(callable).get();
            if (CollectionUtils.isNotEmpty(jobs)) {
                jobs.forEach(a -> logger.info(a.toString()));
                setJobStatusInfo(jobs);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new GATEException(e);
        }
    }

    @Override
    public Boolean isValid() throws GATEException {
        logger.debug("ENTERING isValid()");
        try {
            String results = SSHConnectionUtil.execute("ls /projects/mapseq | wc -l", getSite().getUsername(),
                    getSite().getSubmitHost());
            if (StringUtils.isNotEmpty(results) && Integer.valueOf(results.trim()) > 0) {
                return true;
            }
        } catch (NumberFormatException | JLRMException e) {
            throw new GATEException(e);
        }
        return false;
    }

    @Override
    public List<GlideinMetric> getMetrics() throws GATEException {
        logger.debug("ENTERING lookupMetrics()");
        Map<String, GlideinMetric> metricsMap = new HashMap<String, GlideinMetric>();
        List<Queue> queueList = getSite().getQueueList();
        queueList.forEach(a -> metricsMap.put(a.getName(), new GlideinMetric(getSite().getName(), a.getName(), 0, 0)));
        try {
            String jobName = String.format("glidein-%s", getSite().getName().toLowerCase());
            if (CollectionUtils.isNotEmpty(getJobStatusInfo())) {
                for (JobStatusInfo info : getJobStatusInfo()) {
                    if (!info.getJobName().equals(jobName)) {
                        continue;
                    }
                    SLURMJobStatusType status = SLURMJobStatusType.valueOf(info.getStatus());
                    switch (status) {
                        case PENDING:
                            metricsMap.get(info.getQueue()).incrementPending();
                            break;
                        case RUNNING:
                            metricsMap.get(info.getQueue()).incrementRunning();
                            break;
                    }
                }
            }

        } catch (Exception e) {
            throw new GATEException(e);
        }

        List<GlideinMetric> metricList = new ArrayList<GlideinMetric>();
        metricList.addAll(metricsMap.values());

        return metricList;
    }

    @Override
    public void createGlidein(Queue queue) throws GATEException {
        logger.debug("ENTERING createGlidein(Queue)");
        try {
            logger.info(getSite().toString());
            logger.info(queue.toString());
            HatterasSubmitCondorGlideinCallable callable = new HatterasSubmitCondorGlideinCallable();
            callable.setCollectorHost(getCollectorHost());
            callable.setUsername(System.getProperty("user.name"));
            callable.setSite(getSite());
            callable.setJobName(String.format("glidein-%s", getSite().getName().toLowerCase()));
            callable.setQueue(queue);
            callable.setRequiredMemory("90000");
            callable.setNumberOfProcessors(this.numberOfProcessors);
            callable.setHostAllowRead(getHostAllow());
            callable.setHostAllowWrite(getHostAllow());
            Executors.newSingleThreadExecutor().submit(callable).get();
        } catch (Exception e) {
            throw new GATEException(e);
        }
    }

    @Override
    public void deleteGlidein(Queue queue) throws GATEException {
        logger.debug("ENTERING deleteGlidein(Queue)");
        try {
            logger.info("siteInfo: {}", getSite());
            logger.info("queueInfo: {}", queue);
            String jobName = String.format("glidein-%s", getSite().getName().toLowerCase());
            if (CollectionUtils.isNotEmpty(getJobStatusInfo())) {
                for (JobStatusInfo info : getJobStatusInfo()) {
                    if (!info.getJobName().equals(jobName)) {
                        continue;
                    }
                    if (!info.getStatus().equals(SLURMJobStatusType.RUNNING)) {
                        continue;
                    }
                    logger.info("deleting: {}", info.toString());
                    SLURMSSHKillCallable killCallable = new SLURMSSHKillCallable(getSite(), info.getJobId());
                    Executors.newSingleThreadExecutor().submit(killCallable).get();
                    // only delete one...engine will trigger next deletion
                    break;
                }
            }
        } catch (Exception e) {
            throw new GATEException(e);
        }
    }

    @Override
    public void deletePendingGlideins() throws GATEException {
        logger.debug("ENTERING deletePendingGlideins()");
        try {
            String jobName = String.format("glidein-%s", getSite().getName().toLowerCase());
            if (CollectionUtils.isNotEmpty(getJobStatusInfo())) {
                for (JobStatusInfo info : getJobStatusInfo()) {
                    if (!info.getJobName().equals(jobName)) {
                        continue;
                    }
                    if (info.getStatus().equals(SLURMJobStatusType.PENDING.toString())) {
                        logger.info("deleting: {}", info.toString());
                        SLURMSSHKillCallable killCallable = new SLURMSSHKillCallable(getSite(), info.getJobId());
                        Executors.newSingleThreadExecutor().submit(killCallable).get();
                        // throttle the deleteGlidein calls such that SSH doesn't complain
                        Thread.sleep(2000);
                    }
                }
            }
        } catch (Exception e) {
            throw new GATEException(e);
        }
    }

    public String getNumberOfProcessors() {
        return numberOfProcessors;
    }

    public void setNumberOfProcessors(String numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
    }

}
