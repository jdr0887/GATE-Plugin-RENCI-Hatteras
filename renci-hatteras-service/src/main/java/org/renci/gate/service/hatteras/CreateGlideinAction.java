package org.renci.gate.service.hatteras;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.renci.jlrm.slurm.ssh.SLURMSSHJob;

@Command(scope = "renci-hatteras", name = "create-glidein", description = "Create Glidein")
@Service
public class CreateGlideinAction implements Action {

    @Option(name = "--username", required = true, multiValued = false)
    private String username;

    @Option(name = "--submitHost", required = true, multiValued = false)
    private String submitHost;

    @Option(name = "--queueName", required = true, multiValued = false)
    private String queueName;

    @Option(name = "--collectorHost", required = true, multiValued = false)
    private String collectorHost;

    @Option(name = "--hostAllow", required = false, multiValued = false)
    private String hostAllow;

    @Option(name = "--numberOfProcessors", required = false, multiValued = false)
    private String numberOfProcessors = "$(DETECTED_CORES)/2";

    public CreateGlideinAction() {
        super();
    }

    @Override
    public Object execute() {
        Site site = new Site();
        site.setName("Hatteras");
        site.setSubmitHost(submitHost);
        site.setUsername(username);

        Queue queue = new Queue();
        queue.setName(queueName);
        queue.setRunTime(5760L);
        queue.setNumberOfProcessors(16);

        try {
            HatterasSubmitCondorGlideinCallable callable = new HatterasSubmitCondorGlideinCallable();
            callable.setCollectorHost(collectorHost);
            callable.setUsername(System.getProperty("user.name"));
            callable.setSite(site);
            callable.setJobName(String.format("glidein-%s", site.getName().toLowerCase()));
            callable.setQueue(queue);
            callable.setRequiredMemory("90000");
            // *.renci.org
            callable.setNumberOfProcessors(numberOfProcessors);
            callable.setHostAllowRead(hostAllow);
            callable.setHostAllowWrite(hostAllow);
            SLURMSSHJob job = callable.call();
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

        return null;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getSubmitHost() {
        return submitHost;
    }

    public void setSubmitHost(String submitHost) {
        this.submitHost = submitHost;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getCollectorHost() {
        return collectorHost;
    }

    public void setCollectorHost(String collectorHost) {
        this.collectorHost = collectorHost;
    }

    public String getHostAllow() {
        return hostAllow;
    }

    public void setHostAllow(String hostAllow) {
        this.hostAllow = hostAllow;
    }

    public String getNumberOfProcessors() {
        return numberOfProcessors;
    }

    public void setNumberOfProcessors(String numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
    }

}
