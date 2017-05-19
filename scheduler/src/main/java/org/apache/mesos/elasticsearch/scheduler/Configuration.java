package org.apache.mesos.elasticsearch.scheduler;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.elasticsearch.common.Discovery;
import org.apache.mesos.elasticsearch.common.cli.ElasticsearchCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.ZookeeperCLIParameter;
import org.apache.mesos.elasticsearch.common.cli.validators.CLIValidators;
import org.apache.mesos.elasticsearch.common.util.NetworkUtils;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.IpPortsListZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.MesosZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.formatter.ZKFormatter;
import org.apache.mesos.elasticsearch.common.zookeeper.parser.ZKAddressParser;
import org.apache.mesos.elasticsearch.scheduler.state.ClusterState;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * Holder object for framework configuration.
 */
@SuppressWarnings("PMD.TooManyFields")
public class Configuration {
    // **** ELASTICSEARCH
    public static final String ELASTICSEARCH_CPU = "--elasticsearchCpu";
    public static final String ELASTICSEARCH_RAM = "--elasticsearchRam";
    public static final String ELASTICSEARCH_HEAP_RAM = "--elasticsearchHeap";
    public static final String ELASTICSEARCH_DISK = "--elasticsearchDisk";
    // **** WEB UI
    public static final String WEB_UI_PORT = "--webUiPort";
    public static final String FRAMEWORK_NAME = "--frameworkName";
    public static final String EXECUTOR_NAME = "--executorName";
    public static final String EXECUTOR_LABELS = "--executorLabels";
    public static final String DATA_DIR = "--dataDir";
    public static final String DEFAULT_HOST_DATA_DIR = "/var/lib/mesos/slave/elasticsearch";
    // DCOS Certification requirement 01
    public static final String FRAMEWORK_FAILOVER_TIMEOUT = "--frameworkFailoverTimeout";
    // DCOS Certification requirement 13
    public static final String FRAMEWORK_ROLE = "--frameworkRole";
    public static final String EXECUTOR_IMAGE = "--elasticsearchDockerImage";
    public static final String EXECUTOR_VERSION = "--elasticVersion";
    public static final String EXECUTOR_BINARY = "--elasticsearchBinaryUrl";
    public static final String DEFAULT_EXECUTOR_IMAGE = "elasticsearch:latest";
    public static final String EXECUTOR_FORCE_PULL_IMAGE = "--executorForcePullImage";
    public static final String FRAMEWORK_PRINCIPAL = "--frameworkPrincipal";
    public static final String FRAMEWORK_SECRET_PATH = "--frameworkSecretPath";
    public static final String ES_TAR = "public/elasticsearch.tar.gz";
    public static final String ES_BINARY = "./elasticsearch-*/bin/elasticsearch";
    private static final Logger LOGGER = Logger.getLogger(Configuration.class);
    public static final String FRAMEWORK_USE_DOCKER = "--frameworkUseDocker";
    public static final String JAVA_HOME = "--javaHome";
    public static final String USE_IP_ADDRESS = "--useIpAddress";
    public static final String ELASTICSEARCH_PORTS = "--elasticsearchPorts";
    public static final String CONTAINER_PATH_DATA = "/usr/share/elasticsearch/data";
    public static final String CONTAINER_PATH_CONF = "/usr/share/elasticsearch/config";
    public static final String CONTAINER_PATH_CONF_YML = CONTAINER_PATH_CONF + "/elasticsearch.yml";
    public static final String HOST_SANDBOX = "./."; // Due to some protobuf weirdness. Requires './.' Not just '.'
    public static final String HOST_PATH_HOME = HOST_SANDBOX + "/es_home";
    public static final String HOST_PATH_CONF = HOST_SANDBOX;
    // **** External Volumes
    public static final String EXTERNAL_VOLUME_DRIVER = "--externalVolumeDriver";
    public static final String EXTERNAL_VOLUME_OPTIONS = "--externalVolumeOptions";

    // **** ZOOKEEPER
    private final ZookeeperCLIParameter zookeeperCLI = new ZookeeperCLIParameter();
    private final ElasticsearchCLIParameter elasticsearchCLI = new ElasticsearchCLIParameter();
    @Parameter(names = {ELASTICSEARCH_CPU}, description = "The amount of CPU resource to allocate to the elasticsearch instance.", validateValueWith = CLIValidators.PositiveDouble.class)
    private double cpus = 1.0;
    @Parameter(names = {ELASTICSEARCH_RAM}, description = "The amount of ram resource to allocate to the elasticsearch instance (MB).", validateValueWith = CLIValidators.PositiveDouble.class)
    private double mem = 256;
    @Parameter(names = {ELASTICSEARCH_HEAP_RAM}, description = "The amount of ram resource to allocate to the elasticsearch instance (MB).", validateValueWith = CLIValidators.PositiveDouble.class)
    private double heapMem = 128;
    @Parameter(names = {ELASTICSEARCH_DISK}, description = "The amount of Disk resource to allocate to the elasticsearch instance (MB).", validateValueWith = CLIValidators.PositiveDouble.class)
    private double disk = 1024;
    @Parameter(names = {WEB_UI_PORT}, description = "TCP port for web ui interface.", validateValueWith = CLIValidators.PositiveInteger.class)
    private int webUiPort = 31100; // Default is more likely to work on a default Mesos installation
    @Parameter(names = {ELASTICSEARCH_PORTS}, description = "Override Mesos provided ES HTTP and transport ports. Format `HTTP_PORT,TRANSPORT_PORT` (comma delimited, both required).", validateWith = CLIValidators.NumericListOfSizeTwo.class)
    private String elasticsearchPorts = ""; // Defaults to Mesos specified ports.

    // **** FRAMEWORK
    private String version = "1.0.1";
    @Parameter(names = {FRAMEWORK_NAME}, description = "The name given to the framework.", validateWith = CLIValidators.NotEmptyString.class)
    private String frameworkName = "elasticsearch";
    @Parameter(names = {EXECUTOR_NAME}, description = "The name given to the executor task.", validateWith = CLIValidators.NotEmptyString.class)
    private String executorName = "elasticsearch-executor";
    @Parameter(names = {EXECUTOR_LABELS}, description = "One or more labels given to the executor task." +
        "E.g. 'environment=prod bananas=apples'", variableArity = true)
    private List<String> executorLabels = new ArrayList<>();
    @Parameter(names = {DATA_DIR}, description = "The host data directory used by Docker volumes in the executors. [DOCKER MODE ONLY]")
    private String dataDir = DEFAULT_HOST_DATA_DIR;
    @Parameter(names = {FRAMEWORK_FAILOVER_TIMEOUT}, description = "The time before Mesos kills a scheduler and tasks if it has not recovered (ms).", validateValueWith = CLIValidators.PositiveDouble.class)
    private double frameworkFailoverTimeout = 2592000; // Mesos will kill framework after 1 month if marathon does not restart.
    @Parameter(names = {FRAMEWORK_ROLE}, description = "Used to group frameworks for allocation decisions, depending on the allocation policy being used.", validateWith = CLIValidators.NotEmptyString.class)
    private String frameworkRole = "*"; // This is the default if none is passed to Mesos
    @Parameter(names = {EXECUTOR_IMAGE}, description = "The elasticsearch docker image to use. E.g. 'elasticsearch:latest' [DOCKER MODE ONLY]", validateWith = CLIValidators.NotEmptyString.class)
    private String executorImage = DEFAULT_EXECUTOR_IMAGE;
    @Parameter(names = {EXECUTOR_VERSION}, description = "The elasticseach version. E.g. '5.0'")
    private String executorVersion = "2.4";
    @Parameter(names = {EXECUTOR_BINARY}, description = "The elasticsearch binary to use (Must be tar.gz format). " +
            "E.g. 'https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.2.0/elasticsearch-2.2.0.tar.gz' [JAR MODE ONLY]", validateWith = CLIValidators.NotEmptyString.class)
    private String executorBinary = "";
    @Parameter(names = {EXECUTOR_FORCE_PULL_IMAGE}, arity = 1, description = "Option to force pull the executor image. [DOCKER MODE ONLY]")
    private Boolean executorForcePullImage = false;
    @Parameter(names = {FRAMEWORK_PRINCIPAL}, description = "The principal to use when registering the framework (username).")
    private String frameworkPrincipal = "";
    @Parameter(names = {FRAMEWORK_SECRET_PATH}, description = "The path to the file which contains the secret for the principal (password). Password in file must not have a newline.")
    private String frameworkSecretPath = "";
    @Parameter(names = {FRAMEWORK_USE_DOCKER}, arity = 1, description = "The framework will use docker if true, or jar files if false. If false, the user must ensure that the scheduler jar is available to all slaves.")
    private Boolean isFrameworkUseDocker = true;
    private InetSocketAddress frameworkFileServerAddress;
    @Parameter(names = {JAVA_HOME}, description = "When starting in jar mode, if java is not on the path, you can specify the path here. [JAR MODE ONLY]", validateWith = CLIValidators.NotEmptyString.class)
    private String javaHome = "";
    @Parameter(names = {USE_IP_ADDRESS}, arity = 1, description = "If true, the framework will resolve the local ip address. If false, it uses the hostname.")
    private Boolean isUseIpAddress = false;

    // **** External Volumes
    @Parameter(names = {EXTERNAL_VOLUME_DRIVER}, description = "Use external volume storage driver. By default, nodes will use volumes on host.")
    private String externalVolumeDriver = "";
    @Parameter(names = {EXTERNAL_VOLUME_OPTIONS}, description = "External volume driver options.")
    private String externalVolumeOption = "";

    // ****************** Runtime configuration **********************
    public Configuration(String... args) {
        final JCommander jCommander = new JCommander();
        jCommander.addObject(zookeeperCLI);
        jCommander.addObject(elasticsearchCLI);
        jCommander.addObject(this);
        try {
            jCommander.parse(args); // Parse command line args into configuration class.
        } catch (com.beust.jcommander.ParameterException ex) {
            System.out.println(ex);
            jCommander.setProgramName("(Options preceded by an asterisk are required)");
            jCommander.usage();
            throw ex;
        }
    }

    public double getCpus() {
        return cpus;
    }

    public double getMem() {
        return mem;
    }

    public double getHeapMem() {
        return heapMem;
    }

    public double getDisk() {
        return disk;
    }

    public int getElasticsearchNodes() {
        return elasticsearchCLI.getElasticsearchNodes();
    }

    public void setElasticsearchNodes(int numberOfNodes) throws IllegalArgumentException {
        elasticsearchCLI.setElasticsearchNodes(numberOfNodes);
    }

    public String getElasticsearchSettingsLocation() {
        return elasticsearchCLI.getElasticsearchSettingsLocation();
    }

    public String getElasticsearchClusterName() {
        return elasticsearchCLI.getElasticsearchClusterName();
    }

    public int getWebUiPort() {
        return webUiPort;
    }

    public String getVersion() {
        return version;
    }

    public String getFrameworkName() {
        return frameworkName;
    }

    public String getTaskName() {
        return executorName;
    }

    public Map<String, String> getTaskLabels() {
        HashMap<String, String> map = new HashMap<>();
        for (String keyValue : executorLabels) {
            String[] kvp = keyValue.split("=", 2);
            if (kvp.length == 2) {
              map.put(kvp[0], kvp[1]);
            }
        }

        return map;
    }

    public String getDataDir() {
        return dataDir;
    }

    public double getFailoverTimeout() {
        return frameworkFailoverTimeout;
    }

    public String getFrameworkRole() {
        return frameworkRole;
    }

    public String getExecutorImage() {
        return executorImage;
    }

    public String getExecutorVersion() {
        return executorVersion;
    }

    public Boolean getExecutorForcePullImage() {
        return executorForcePullImage;
    }

    public Boolean getIsUseIpAddress() {
        return isUseIpAddress;
    }

    public String getElasticsearchBinary() {
        return executorBinary;
    }

    // ******* Helper methods
    public String getMesosStateZKURL() {
        ZKFormatter mesosStateZKFormatter = new IpPortsListZKFormatter(new ZKAddressParser());
        return mesosStateZKFormatter.format(zookeeperCLI.getZookeeperMesosUrl());
    }

    public String getMesosZKURL() {
        ZKFormatter mesosZKFormatter = new MesosZKFormatter(new ZKAddressParser());
        return mesosZKFormatter.format(zookeeperCLI.getZookeeperMesosUrl());
    }

    public ZookeeperCLIParameter getZookeeperCLI() {
        return zookeeperCLI;
    }

    public ElasticsearchCLIParameter getElasticsearchCLI() {
        return elasticsearchCLI;
    }

    public String getFrameworkSecretPath() {
        return frameworkSecretPath;
    }

    public String getFrameworkPrincipal() {
        return frameworkPrincipal;
    }

    public Boolean isFrameworkUseDocker() {
        return isFrameworkUseDocker;
    }

    public String getFrameworkFileServerAddress() {
        String result = "";
        if (frameworkFileServerAddress != null) {
            return NetworkUtils.addressToString(frameworkFileServerAddress, getIsUseIpAddress());
        }
        return result;
    }

    public String webUiAddress() {
        return NetworkUtils.addressToString(NetworkUtils.hostSocket(getWebUiPort()), getIsUseIpAddress());
    }

    public void setFrameworkFileServerAddress(InetSocketAddress addr) {
        if (addr != null) {
            frameworkFileServerAddress = addr;
        } else {
            LOGGER.error("Could not set webserver address. Was null.");
        }
    }

    public String getJavaHome() {
        if (!javaHome.isEmpty()) {
            return javaHome.replaceAll("java$", "").replaceAll("/$", "") + "/";
        } else {
            return "";
        }
    }

    public List<Integer> getElasticsearchPorts() {
        if (elasticsearchPorts.isEmpty()) {
            return Collections.emptyList();
        }
        String[] portsRaw = elasticsearchPorts.replace(" ", "").split(",");
        ArrayList<Integer> portsList = new ArrayList<>(2);
        for (String port : portsRaw) {
            portsList.add(Integer.parseInt(port));
        }
        return portsList;
    }

    public String getExternalVolumeDriver() {
        return externalVolumeDriver;
    }

    public String getExternalVolumeOption() {
        return externalVolumeOption;
    }

    public String nativeCommand(List<String> arguments) {
        String folders = getDataDir() + " " + HOST_SANDBOX;
        String mkdir = "mkdir -p " + folders + "; ";
        String chown = "chown -R nobody " + folders + "; ";
        return mkdir +
                chown +
                " su -s /bin/sh -c \""
                + Configuration.ES_BINARY
                + " "
                + arguments.stream().collect(Collectors.joining(" "))
                + "\" nobody";
    }

    @SuppressWarnings("PMD.AvoidUsingHardCodedIP")
    public List<String> esArguments(ClusterState clusterState, Protos.DiscoveryInfo discoveryInfo, Protos.SlaveID slaveID) {
        List<String> args = new ArrayList<>();
        List<Protos.TaskInfo> taskList = clusterState.getTaskList();
        String hostAddress = "";
        if (taskList.size() > 0) {
            Protos.TaskInfo taskInfo = taskList.get(0);
            String taskId = taskInfo.getTaskId().getValue();
            InetSocketAddress transportAddress = clusterState.getGuiTaskList().get(taskId).getTransportAddress();
            hostAddress = NetworkUtils.addressToString(transportAddress, getIsUseIpAddress()).replace("http://", "");
        }
        addArgsForVersion(args, "bin/elasticsearch", "5.4");
        addIfNotEmpty(args, "default.discovery.zen.ping.unicast.hosts", hostAddress);
        addArgs(args, "default.http.port", String.valueOf(discoveryInfo.getPorts().getPorts(Discovery.CLIENT_PORT_INDEX).getNumber()));
        addArgs(args, "default.transport.tcp.port", String.valueOf(discoveryInfo.getPorts().getPorts(Discovery.TRANSPORT_PORT_INDEX).getNumber()));
        addArgs(args, "default.cluster.name", getElasticsearchClusterName());
        addArgs(args, "default.node.master", "true");
        addArgs(args, "default.node.data", "true");
        addArgsForVersion(args, "default.node.local", "false", "2.");
        addArgsForVersion(args, "default.index.number_of_replicas", "0", "2.");
        addArgsForVersion(args, "default.index.auto_expand_replicas", "0-all", "2.");
        if (!isFrameworkUseDocker()) {
            String taskSpecificDataDir = taskSpecificHostDir(slaveID);
            addArgs(args, "path.home", HOST_PATH_HOME); // Cannot be overidden
            addArgs(args, "default.path.data", taskSpecificDataDir);
            addArgs(args, "path.conf", HOST_PATH_CONF); // Cannot be overidden
        } else {
            addArgs(args, "path.data", CONTAINER_PATH_DATA); // Cannot be overidden
        }
        addArgsForVersion(args, "default.bootstrap.mlockall", "true", "2.");
        addArgsForVersion(args, "default.bootstrap.memory_lock", "true", "5.");
        addArgs(args, "default.network.bind_host", "0.0.0.0");
        addArgsForVersion(args, "default.network.publish_host", "_non_loopback:ipv4_", "2.");
        addArgs(args, "default.gateway.recover_after_nodes", "1");
        addArgs(args, "default.gateway.expected_nodes", "1");
        addArgs(args, "default.indices.recovery.max_bytes_per_sec", "100mb");
        addArgs(args, "default.discovery.type", "zen");
        addArgs(args, "default.discovery.zen.fd.ping_timeout", "30s");
        addArgs(args, "default.discovery.zen.fd.ping_interval", "1s");
        addArgs(args, "default.discovery.zen.fd.ping_retries", "30");
        addArgsForVersion(args, "default.discovery.zen.ping.multicast.enabled", "false", "2.");


        return args;
    }

    public String taskSpecificHostDir(Protos.SlaveID slaveID) {
        return getDataDir() + "/" + getElasticsearchClusterName() + "/" + slaveID.getValue();
    }

    private void addIfNotEmpty(List<String> args, String key, String value) {
        if (!value.isEmpty()) {
            addArgs(args, key, value);
        }
    }

    private void addArgsForVersion(List<String> args, String key, String value, String versionPrefix) {
        if (StringUtils.startsWith(this.executorVersion, versionPrefix)) {
            this.addArgs(args, key, value);
        }
    }

    private void addArgsForVersion(List<String> args, String value, String versionPrefix) {
        if (StringUtils.startsWith(this.executorVersion, versionPrefix)) {
            args.add(value);
        }
    }

    private void addArgs(List<String> args, String key, String value) {
        if (StringUtils.startsWith(this.executorVersion, "5.")) {
            // for es 5
            if (StringUtils.startsWith(key, "index.") || StringUtils.startsWith(key, "default.index.")) {
                // es 5 not allow set index option from arguments
                return;
            }
            if (StringUtils.startsWith(this.executorVersion, "5.4") && StringUtils.startsWith(key, "default")) {
                // es 5.4.0 does not support default arguments
                args.add("-E" + key.replaceFirst("default.", "") + "=" + value);
            } else {
                args.add("-E" + key + "=" + value);
            }
        } else {
            args.add("--" + key + "=" + value);
        }
    }

    public String dataVolumeName(Long nodeId) {
        return getFrameworkName() + nodeId + "data";
    }
}
