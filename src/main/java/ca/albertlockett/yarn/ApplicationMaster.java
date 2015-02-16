package ca.albertlockett.yarn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationMaster {
	
	private final static Logger log = 
			LoggerFactory.getLogger(ApplicationMaster.class);

	private AMRMClientAsync<ContainerRequest> amRmClient;
	private NMClientAsync nmClient;
	private YarnConfiguration conf;
	private FileSystem fileSystem;
	private Path inputFile;
	private Path outputFile;
	private List<BlockLocation> blockList = new ArrayList<BlockLocation>();
	private RMCallbackHandler rmCallbackHandler;
	private NMCallbackHandler containerListener;
	private Integer numOfContainers = new Integer(0);
	private AtomicInteger numCompletedContainers = new AtomicInteger();
	List<Thread> launchThreads = new ArrayList<Thread>();
	private volatile Boolean done = false;
	
	
	// GETTERS AND SETTERS
	public AMRMClientAsync<ContainerRequest> getAmRmClient() {
		return this.amRmClient;
	}
	public NMClientAsync getNmClient() {
		return this.nmClient;
	}
	public Path getInputFile() {
		return this.inputFile;
	}
	public Path getOutputFile() {
		return this.outputFile;
	}
	public List<BlockLocation> getBlockList() {
		return this.blockList;
	}
	public Integer getNumOfContainers() {
		return this.numOfContainers;
	}
	public AtomicInteger getNumcompletedContainers() {
		return this.numCompletedContainers;
	}
	public List<Thread> getLaunchThreads() {
		return this.launchThreads;
	}
	public Boolean isDone() {
		return this.done;
	}
	public void setDone(boolean done) {
		this.done = done;
	}
	
	
	// CONSTRUCTOR
	public ApplicationMaster(List<String> args) throws IOException {
		
		this.conf = new YarnConfiguration();
		
		this.fileSystem = FileSystem.get(conf);
		
		this.inputFile = new Path(args.get(0));
		this.outputFile = new Path(args.get(1));
		
	}
	
	
	
	public void run() throws YarnException, IOException {
		
		this.rmCallbackHandler = new RMCallbackHandler(this);
		
		this.amRmClient = AMRMClientAsync.createAMRMClientAsync(10000, 
				rmCallbackHandler);
		this.amRmClient.init(conf);
		this.amRmClient.start();
		
		// Register with Resource Manager
		RegisterApplicationMasterResponse response = amRmClient.
				registerApplicationMaster(NetUtils.getHostname(),-1,"");
		Log.info("ApplicationMaster is registered with response: {}",
				response.toString());
		
		// Start the NMClientService
		this.containerListener = new NMCallbackHandler(this);
		this.nmClient 
			= NMClientAsync.createNMClientAsync(this.containerListener);
		this.nmClient.init(this.conf);
		this.nmClient.start();
		log.info("Starting NM Client");
		
		
		// Ask for some containers
		Resource capacity = Records.newRecord(Resource.class);
		Priority priority = Records.newRecord(Priority.class);
		capacity.setMemory(128);
		priority.setPriority(0);
		
		BlockLocation[] blocks = this.getBlockLocations();
		for(BlockLocation block : blocks) { blockList.add(block); }
		log.info("Number of blocks to process = {}", blockList.size());
		
		for(BlockLocation block : blockList) {
			ContainerRequest ask = new ContainerRequest(capacity, 
					block.getHosts(), null, priority);
			this.amRmClient.addContainerRequest(ask);
			this.numOfContainers++;
			log.info("Requesting container for block {}", block.toString());
		}
		
		
		while(!done 
				&& this.numCompletedContainers.get() < this.numOfContainers) {
			
			log.info("completed containers:{}/{}", 
					this.numCompletedContainers.get(),
					this.numOfContainers);
			
			try {
				Thread.sleep(2000);
			} catch(InterruptedException e) {
				log.error("SUCCESS: EXCEPTION WAS CAUGHT");
				log.error(e.getMessage());
				e.printStackTrace();
			}
		}
		
		for(Thread thread : launchThreads) {
			try{
				thread.join(100000);
			} catch(InterruptedException e) {
				log.error("SUCCESS: EXCEPTION CAUGHT JOINING THREADS");
				log.error(e.getMessage());
				e.printStackTrace();
			}
		}
		
		
		// Stop NM Client service
		this.nmClient.stop();
		
		// Unregister application
		amRmClient.unregisterApplicationMaster(
				FinalApplicationStatus.SUCCEEDED,
				"Applicatoin Completed", null);
		
		// Wait for un-registration to happen
		while(!amRmClient.isInState(STATE.STOPPED)) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				log.error("Interrupted waiting for application to finish");
				log.error(e.getMessage());
				e.printStackTrace();
			}
		}
		
		// Stop Application
		amRmClient.stop();
	}
	
	public static void main(String[] args) {
		log.info("Starting the Application Master");
		ApplicationMaster appMaster;
		
		try {
			
			appMaster = new ApplicationMaster(Arrays.asList(args));
			appMaster.run();
			
		} catch(IOException e) {
			log.error("SUCCESS: IOException caught stargin AppMAster");
			log.error(e.getMessage());
			e.printStackTrace();
		} catch(YarnException e) {
			log.error("SUCCESS: Yarn Exccetion caught starting AppMaster");
			log.error(e.getMessage());
			e.printStackTrace();
		}
		
		
	}
	
	
	
	private BlockLocation[] getBlockLocations() throws IOException {
		
		FileStatus fileStatus = fileSystem.getFileStatus(this.inputFile);
		Log.info("Input File Status = {}", fileStatus);
		BlockLocation[] blocks = fileSystem.getFileBlockLocations(
				fileStatus, 0, fileStatus.getLen());
		log.info("Number of blocks for {} = {}", 
				inputFile.toString(), blocks.length);
		return blocks;
	}
	
	
}
