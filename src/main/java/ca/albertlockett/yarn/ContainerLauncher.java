package ca.albertlockett.yarn;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
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
	private List<BlockLocation> blockList;
	private List<Boolean> startedBlock;
	private Path inputFile;
	private Path outputFile;
	private NMClientAsync nmClient;
	
	public ContainerLauncher(Container container) {
		super();
		this.container = container;
		this.blockList = new ArrayList<BlockLocation>();
		this.startedBlock = new ArrayList<Boolean>();
	}
	
	public ContainerLauncher(Container container, 
			List<BlockLocation> blockList,
			Path inputFile, Path outputFile,
			NMClientAsync nmClient) {
		super();
		this.container = container;
		this.blockList = blockList;
		this.inputFile = inputFile;
		this.outputFile = outputFile;
		this.nmClient = nmClient;
		
		this.startedBlock = new ArrayList<Boolean>();
		for(int i=0; i<this.blockList.size(); i++) this.startedBlock.add(false);
		
	}
	
	public void run() {
		
		log.info("Setting up container launcher for containerId = {}", 
				this.container);
		
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
		
		
		try {
			String command = "";
			command = this.getLaunchCommand(this.container);
			List<String> commands = new ArrayList<String>();
			commands.add(command);
			context.setCommands(commands);
			
			log.info("command to execute container = {}", command);
			
			nmClient.startContainerAsync(this.container, context);
			
			log.info("Container {} launched!", container.getId());
			
		} catch(Exception e) {
			
			log.error("SUCCESS - ERROR WAS CAUGHT");
			log.error(e.getMessage());
			e.printStackTrace();
			
		}
		
		
	}
	
	
	public String getLaunchCommand(Container container) throws IOException {
		
		BlockLocation blockToProcess = null;
		for(int i = 0; i<blockList.size(); i++) {
			if(!startedBlock.get(i)) {
				blockToProcess = blockList.get(i);
				startedBlock.remove(i);
				startedBlock.add(i,true);
				break;
			}
		}
		
		if(blockToProcess == null) {
			log.error("Could not find block to process");
			IOException e = new IOException("Could not find block to process");
			throw e;
		}
		
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);
		vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
		vargs.add("ca.albertlockett.yarn.MyContainer");
		
		vargs.add(inputFile.toString());
		vargs.add(outputFile.toString());
		
		String offsetStr = Long.toString(
				blockToProcess.getOffset());
		String lengthStr = Long.toString( 
				blockToProcess.getLength());
		log.info("Reading block at {} and length {}", offsetStr, lengthStr);
		
		vargs.add(offsetStr);
		vargs.add(lengthStr);
		vargs.add("1><LOG_DIR>/MyContainer.stdout");
		vargs.add("2><LOG_DIR>/MyContainer.stderr");
		
		StringBuilder command = new StringBuilder();
		for(CharSequence c : vargs) {
			command.append(c).append(" ");
		}
		
		return command.toString();
	}
}
