package ca.albertlockett.yarn;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppClient {
	
	private final static Logger log = 
			LoggerFactory.getLogger(AppClient.class);

	private static final String APP_NAME = "App";
	private static final String appJar = "app.jar";
	private YarnConfiguration conf;
	private YarnClient yarnClient;
	private FileSystem fs;
	private Path inputPath;
	private Path outputPath;
	private ApplicationId appId;
	
	
	// CONSTRUCTOR
	public AppClient(String[] args) throws IOException {
		
		this.conf = new YarnConfiguration();
		
		this.yarnClient = YarnClient.createYarnClient();
		this.yarnClient.init(conf);
		this.yarnClient.start();
		
		this.fs = FileSystem.get(conf);
		
		this.inputPath = new Path(args[0]);
		this.outputPath = new Path(args[1]);
		
	}

	
	public boolean run() throws YarnException, IOException {
		
		log.info("STARTING APPLICATION ---");
		
		// Create Application
		GetNewApplicationResponse appResponse = 
				this.yarnClient.createApplication().getNewApplicationResponse();
		
		this.appId = appResponse.getApplicationId();
		
		int maxClusterMemory = 
				appResponse.getMaximumResourceCapability().getMemory();
		int maxVCores = 
				appResponse.getMaximumResourceCapability().getVirtualCores();
		log.info("Max Memory = {}\tMax V-Cores = {}",
				maxClusterMemory, maxVCores);
		
		YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
		
		int nodeManagers = clusterMetrics.getNumNodeManagers();
		log.info("Number of node managers = {}", nodeManagers);
		
		List<NodeReport> reports = yarnClient.getNodeReports(NodeState.RUNNING);
		for(NodeReport report : reports) {
			log.info("Node ID = {}, Address = {},  # Containers = {}",
					report.getNodeId(), report.getHttpAddress(),
					report.getNumContainers());
		}
		
		List<QueueInfo> queueReports = yarnClient.getAllQueues();
		for(QueueInfo queue : queueReports) {
			log.info("Queue Name = {}, Capacity = {}, Max Capacity = {}",
					queue.getQueueName(), queue.getCapacity(),
					queue.getMaximumCapacity());
		}
		
		Path src = new Path(appJar);
		String pathSuffix = APP_NAME + "/" + appId.getId() + "/app.jar";
		Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
		fs.copyFromLocalFile(false, true, src, dst);
		FileStatus destStatus = fs.getFileStatus(dst);
		
		LocalResource jarResource = Records.newRecord(LocalResource.class);
		jarResource.setResource(ConverterUtils.getYarnUrlFromPath(dst));
		jarResource.setSize(destStatus.getBlockSize());
		jarResource.setTimestamp(destStatus.getModificationTime());
		jarResource.setType(LocalResourceType.FILE);
		jarResource.setVisibility(LocalResourceVisibility.APPLICATION);
		
		Map<String, LocalResource> localResources = 
				new HashMap<String, LocalResource> ();
		localResources.put("app.jar", jarResource);
		
		Map<String, String> env = new HashMap<String, String>();
		env.put("AMJARTIMESTAMP", Long.toString(jarResource.getTimestamp()));
		env.put("AMJARLEN", Long.toString(jarResource.getSize()));
		
		String appJarDst = dst.toUri().toString();
		env.put("AMJAR", appJarDst);
		log.info("AMJAR environment variable set to {}", appJarDst);
		
		// Configure Launch Environment
		StringBuilder classPathEnv = new StringBuilder();
		classPathEnv.append(File.pathSeparatorChar).append("./app.jar");
		for(String c : conf.getStrings(
				YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) 
		{
			classPathEnv.append(File.pathSeparatorChar);
			classPathEnv.append(c.trim());
		}
		classPathEnv.append(File.pathSeparatorChar);
		classPathEnv.append(Environment.CLASSPATH.$());
		env.put("CLASSPATH", classPathEnv.toString());
		log.info("CLASSPATH variable set to {}", classPathEnv.toString());
		
		
		// Configure the application context
		YarnClientApplication app = this.yarnClient.createApplication();
		ApplicationSubmissionContext appContext = 
				app.getApplicationSubmissionContext();
		appContext.setApplicationName(APP_NAME);
		
		
		// Configure launch context
		ContainerLaunchContext amContainer = 
				Records.newRecord(ContainerLaunchContext.class);
		amContainer.setLocalResources(localResources);
		amContainer.setEnvironment(env);
		
		
		// Configure the command line for the application manager
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);
		vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
		vargs.add("ca.albertlockett.yarn.ApplicationMaster");
		//vargs.add("1><LOG_DIR>/AppClient.stdout");
		//vargs.add("2><LOG_DIR>/AppClient.stderr");
		vargs.add(this.inputPath.toString());
		vargs.add(this.outputPath.toString());
		
		StringBuilder command = new StringBuilder();
		for(CharSequence c : vargs) {
			command.append(c).append(" ");
		}
		List<String> commands = new ArrayList<String>();
		commands.add(command.toString());
		amContainer.setCommands(commands);
		
		log.info("Command to execute app master = {}", command);
		
		// Request memory
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(512);
		appContext.setResource(capability);
		
		// submit container launch context to resource manager
		appContext.setAMContainerSpec(amContainer);
		
		// submit container
		yarnClient.submitApplication(appContext);
		
		return true;
		
	}
	
	
	public static void main(String [] args) { 
	
		AppClient appClient;
		boolean result = false;
		try {
			appClient = new AppClient(args);
			result = appClient.run();
			
		} catch(YarnException e) {
			log.error("SUCCESS: YARN EXCEPTION CAUGHT");
			log.error(e.getMessage());
			e.printStackTrace();
			
		} catch(IOException e) {
			log.error("SUCCESS: IO EXCEPTION CAUGHT");
			log.error(e.getMessage());
			e.printStackTrace();
			
		}
		
		if(result) {
			log.info("Application ran sucessfully");
		}
		
		
	}
	
}
