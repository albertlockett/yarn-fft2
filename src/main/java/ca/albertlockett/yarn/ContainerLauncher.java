package ca.albertlockett.yarn;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerLauncher implements Runnable {

	private final static Logger log = 
			LoggerFactory.getLogger(ContainerLauncher.class);
	
	private Container container;
	private List<BlockStatus> blockList;
	private Path inputFile;
	private Path outputFile;
	private NMClientAsync nmClient;
	
	public ContainerLauncher(Container container) {
		super();
		this.container = container;
		this.blockList = new ArrayList<BlockStatus>();
	}
	
	public ContainerLauncher(Container container, 
			List<BlockStatus> blockList,
			Path inputFile, Path outputFile,
			NMClientAsync nmClient) {
		super();
		this.container = container;
		this.blockList = blockList;
		this.inputFile = inputFile;
		this.outputFile = outputFile;
		this.nmClient = nmClient;
	}
	
	public void run() {
		
		log.info("Setting up container launcher for containerId = {}");
		
		Map<String, LocalResource> localResources = 
				new HashMap<String, LocalResource> ();
		
		Map<String, String> env = System.getenv();
		
		LocalResource appJarFile = Records.newRecord(LocalResource.class);
		appJarFile.setType(LocalResourceType.FILE);
		appJarFile.setVisibility(LocalResourceVisibility.APPLICATION);
		
		try {
			appJarFile.setResource(ConverterUtils.getYarnUrlFromURI(
					new URI(env.get("AMJAR"))));
		} catch(URISyntaxException e) {
			log.error("SUCESS - ERROR WAS CAUGHT SUCESSFULLY");
			log.error(e.getMessage());
			e.printStackTrace();
			return;
		}
		
		appJarFile.setTimestamp(Long.valueOf(env.get("AMJARTIMESTAMP")));
		appJarFile.setSize(Long.valueOf(env.get("AMJARLEN")));
		localResources.put("app.jar", appJarFile);
		
		log.info("Added {} as local resource to container",
				appJarFile.toString());
		
		
		ContainerLaunchContext context = 
				Records.newRecord(ContainerLaunchContext.class);
		context.setEnvironment(env);
		context.setLocalResources(localResources);
		
		String command = "";
		try {
			command = this.getLaunchCommand(this.container);
		} catch(Exception e) {
			log.error("SUCCESS - ERROR WAS CAUGHT");
			log.error(e.getMessage());
			e.printStackTrace();
		}
		
		List<String> commands = new ArrayList<String>();
		commands.add(command);
		context.setCommands(commands);
		
		log.info("command to execute container = {}", command);
		
		nmClient.startContainerAsync(this.container, context);
		
		log.info("Container {} launched!", container.getId());
	}
	
	
	public String getLaunchCommand(Container container) throws IOException {
		
		String hostname = NetUtils.getHostname();
		boolean foundBlock = false;
		BlockStatus blockToProcess = null;
		
		outer: for(BlockStatus current : blockList) {
			synchronized(current) {
				if(!current.isStarted()){
					for(int i = 0; i< current.getLocation().getHosts().length;
							i++) {
						
						String currentHost = current.getLocation()
								.getHosts()[i] + ":8042";
						
						log.info("comparing {} with container on {}",
								currentHost, hostname);
						
						if(currentHost.equals(hostname)) {
							blockToProcess = current;
							current.setStarted(true);
							current.setContainer(container);
							foundBlock = true;
							break outer;
						}
					}
				}
			}
		}
		
		if(foundBlock) {
			log.info("data found locally");
		} else {
			log.info("data not found locally - try another node");
			
			for(BlockStatus current : blockList) {
				if(!current.isStarted()) {
					blockToProcess = current;
					current.setStarted(true);
					current.setContainer(container);
					foundBlock = true;
					break;
				}
			}
		}
		
		// Debugging
		if(blockToProcess == null) {
			log.info("ERROR BLOCK TO PROCESS = NULL");
		} else if(blockToProcess.getLocation() == null) {
			log.info("ERROR BLOCK LOCATION = NULL");
		}
		
		
		
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);
		vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
		vargs.add("ca.albertlockett.MyContainer");
		
		vargs.add(inputFile.toString());
		vargs.add(outputFile.toString());
		
		String offsetStr = Long.toString(
				blockToProcess.getLocation().getOffset());
		String lengthStr = Long.toString( 
				blockToProcess.getLocation().getLength());
		log.info("Reading block at {} and length {}", offsetStr, lengthStr);
		
		vargs.add(offsetStr);
		vargs.add(lengthStr);
		vargs.add("1><LOG_DIR>/MyContainer.stdout");
		vargs.add("2><LOG_DIR</MyContainer.stderr");
		
		StringBuilder command = new StringBuilder();
		for(CharSequence c : vargs) {
			command.append(c).append(" ");
		}
		
		return command.toString();
	}
}
