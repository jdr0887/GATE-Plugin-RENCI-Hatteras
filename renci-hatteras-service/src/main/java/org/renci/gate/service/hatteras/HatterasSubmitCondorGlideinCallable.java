package org.renci.gate.service.hatteras;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.renci.jlrm.slurm.ssh.SLURMSSHJob;
import org.renci.jlrm.slurm.ssh.SLURMSubmitScriptExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HatterasSubmitCondorGlideinCallable implements Callable<SLURMSSHJob> {

    private final Logger logger = LoggerFactory.getLogger(HatterasSubmitCondorGlideinCallable.class);

    private Site site;

    private SLURMSSHJob job;

    private String collectorHost;

    private String requiredMemory;

    private Queue queue;

    private String jobName;

    private String username;

    private String hostAllowRead;

    private String hostAllowWrite;

    private String numberOfProcessors;

    public HatterasSubmitCondorGlideinCallable() {
        super();
    }

    /**
     * Creates a new glide-in
     * 
     * @return number of glide-ins submitted
     */
    public SLURMSSHJob call() throws JLRMException {
        logger.debug("ENTERING call()");

        try {
            Properties velocityProperties = new Properties();
            velocityProperties.put("runtime.log.logsystem.class", "org.apache.velocity.runtime.log.NullLogChute");
            Velocity.init(velocityProperties);
        } catch (Exception e) {
            e.printStackTrace();
        }

        SLURMSSHJob job = new SLURMSSHJob();
        job.setTransferExecutable(Boolean.TRUE);
        job.setTransferInputs(Boolean.TRUE);
        job.setQueueName(this.queue.getName());
        job.setName(this.jobName);
        job.setHostCount(1);
        job.setNumberOfProcessors(getQueue().getNumberOfProcessors());
        job.setOutput(new File("glidein.out"));
        job.setError(new File("glidein.err"));
        job.setWallTime(this.queue.getRunTime());
        job.setMemory(this.requiredMemory);

        VelocityContext velocityContext = new VelocityContext();
        velocityContext.put("collectorHost", this.collectorHost);
        velocityContext.put("jlrmUser", this.username);
        velocityContext.put("jlrmSiteName", getSite().getName());
        velocityContext.put("hostAllowRead", this.hostAllowRead);
        velocityContext.put("hostAllowWrite", this.hostAllowWrite);
        velocityContext.put("numberOfProcessors", this.numberOfProcessors);

        // note that we want a lower max run time here, so that the glidein can shut down
        // gracefully before getting kicked off by the batch scheduler
        long maxRunTimeAdjusted = this.queue.getRunTime() - 20;
        if (maxRunTimeAdjusted < 0) {
            maxRunTimeAdjusted = this.queue.getRunTime() / 2;
        }
        velocityContext.put("siteMaxRunTimeMins", maxRunTimeAdjusted);
        velocityContext.put("siteMaxRunTimeSecs", maxRunTimeAdjusted * 60);
        velocityContext.put("requiredMemory", this.requiredMemory);
        velocityContext.put("glideinStartTime", new Date().getTime());
        velocityContext.put("maxRunTime", maxRunTimeAdjusted);

        try {

            String remoteWorkDirSuffix = String.format(".jlrm/jobs/%s/%s",
                    DateFormatUtils.ISO_DATE_FORMAT.format(new Date()), UUID.randomUUID().toString());
            String command = String.format("(mkdir -p $HOME/%s && echo $HOME)", remoteWorkDirSuffix);
            String remoteHome = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());

            logger.info("remoteHome: {}", remoteHome);
            String remoteWorkDir = String.format("%s/%s", remoteHome, remoteWorkDirSuffix);
            logger.info("remoteWorkDir: {}", remoteWorkDir);
            velocityContext.put("remoteWorkDir", remoteWorkDir);

            File tmpDir = new File(System.getProperty("java.io.tmpdir"));
            File myDir = new File(tmpDir, System.getProperty("user.name"));
            File localWorkDir = new File(myDir, UUID.randomUUID().toString());
            localWorkDir.mkdirs();
            logger.info("localWorkDir: {}", localWorkDir);

            String glideinScriptMacro = IOUtils.toString(this.getClass().getResourceAsStream("glidein.sh.vm"));
            File glideinScript = new File(localWorkDir.getAbsolutePath(), "glidein.sh");
            writeTemplate(velocityContext, glideinScript, glideinScriptMacro);
            job.setExecutable(glideinScript);

            File condorConfigFile = new File(localWorkDir.getAbsolutePath(), "condor_config");
            condorConfigFile.setReadable(true);
            condorConfigFile.setExecutable(true);
            condorConfigFile.setWritable(true, true);
            FileUtils.writeStringToFile(condorConfigFile,
                    IOUtils.toString(this.getClass().getResourceAsStream("condor_config")));
            job.getInputFiles().add(condorConfigFile);

            SLURMSubmitScriptExporter<SLURMSSHJob> exporter = new SLURMSubmitScriptExporter<SLURMSSHJob>();
            this.job = exporter.export(localWorkDir, remoteWorkDir, job);
            SSHConnectionUtil.transferSubmitScript(site.getUsername(), site.getSubmitHost(), remoteWorkDir,
                    this.job.getTransferExecutable(), this.job.getExecutable(), this.job.getTransferInputs(),
                    this.job.getInputFiles(), job.getSubmitFile());

            command = String.format("sbatch %s/%s", remoteWorkDir, job.getSubmitFile().getName());
            String submitOutput = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());

            LineNumberReader lnr = new LineNumberReader(new StringReader(submitOutput));
            String line;
            while ((line = lnr.readLine()) != null) {
                if (line.indexOf("batch job") != -1) {
                    logger.info("line = {}", line);
                    Pattern pattern = Pattern.compile("^.+batch job (\\d*)$");
                    Matcher matcher = pattern.matcher(line);
                    if (!matcher.matches()) {
                        throw new JLRMException("failed to parse the jobid number");
                    } else {
                        job.setId(matcher.group(1));
                    }
                    break;
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            throw new JLRMException("JSchException: " + e.getMessage());
        }

        return job;
    }

    /**
     * Generates a new job file which is used to setup and start the glidein
     */
    private void writeTemplate(VelocityContext velocityContext, File file, String template) throws IOException {
        StringWriter sw = new StringWriter();
        Velocity.evaluate(velocityContext, sw, "glidein", template);
        file.setReadable(true);
        file.setExecutable(true);
        file.setWritable(true, true);
        FileUtils.writeStringToFile(file, sw.toString());
    }

    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
    }

    public SLURMSSHJob getJob() {
        return job;
    }

    public void setJob(SLURMSSHJob job) {
        this.job = job;
    }

    public String getCollectorHost() {
        return collectorHost;
    }

    public void setCollectorHost(String collectorHost) {
        this.collectorHost = collectorHost;
    }

    public String getRequiredMemory() {
        return requiredMemory;
    }

    public void setRequiredMemory(String requiredMemory) {
        this.requiredMemory = requiredMemory;
    }

    public Queue getQueue() {
        return queue;
    }

    public void setQueue(Queue queue) {
        this.queue = queue;
    }

    public String getHostAllowRead() {
        return hostAllowRead;
    }

    public void setHostAllowRead(String hostAllowRead) {
        this.hostAllowRead = hostAllowRead;
    }

    public String getHostAllowWrite() {
        return hostAllowWrite;
    }

    public void setHostAllowWrite(String hostAllowWrite) {
        this.hostAllowWrite = hostAllowWrite;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getNumberOfProcessors() {
        return numberOfProcessors;
    }

    public void setNumberOfProcessors(String numberOfProcessors) {
        this.numberOfProcessors = numberOfProcessors;
    }

}
