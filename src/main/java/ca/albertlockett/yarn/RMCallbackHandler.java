package ca.albertlockett.yarn;

import java.util.List;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RMCallbackHandler implements CallbackHandler {

	private final static Logger log = 
			LoggerFactory.getLogger(RMCallbackHandler.class);
	
	private ApplicationMaster applicationMaster;
	
	
	
	
	// Constructors
	public RMCallbackHandler(ApplicationMaster applicationMaster) {
		this.applicationMaster = applicationMaster;
	}
	
	
	// Callback methods
	public float getProgress() {
		
		int numContainers = 0;
		int completedContainers = 0;
		
		if(applicationMaster.getNumOfContainers() == null 
				|| applicationMaster.getNumcompletedContainers() == null) {
			return (float) 0.00001;
		} else {
			numContainers = applicationMaster.getNumOfContainers();
			completedContainers = applicationMaster
					.getNumcompletedContainers().get();
		}
		
		float progress = (float) completedContainers / (float) numContainers;
		
		if(progress < 0.0){
			progress *= -1.0;
		}
		
		return (float) 0.5;
	}

	public void onContainersAllocated(List<Container> containers) {
		log.info("Got response from RM container ask, allocated count = {}",
				containers.size());
		
		// Start the containers
		for(Container container : containers) {
			
			log.info("Staring container on {}",
					container.getNodeHttpAddress());
			
			ContainerLauncher containerLauncher = new ContainerLauncher(
					container, applicationMaster.getBlockList(),
					applicationMaster.getInputFile(),
					applicationMaster.getOutputFile(),
					applicationMaster.getNmClient());
			
			Thread cntThread  = new Thread(containerLauncher);
			cntThread.run();
			applicationMaster.getLaunchThreads().add(cntThread);
			
		}
		
	}

	public void onContainersCompleted(List<ContainerStatus> statuses) {
		
		log.info("Got a response from RM container ask, complated = {}");
		
		for(ContainerStatus status : statuses) {
			applicationMaster.getNumcompletedContainers().incrementAndGet();
			log.info("container completed sucessfully: {}", 
					status.getContainerId());
		}
		
	}

	public void onError(Throwable arg0) {
		applicationMaster.setDone(true);
		applicationMaster.getAmRmClient().stop();
		
	}

	public void onNodesUpdated(List<NodeReport> arg0) {
		// do nothing
	}

	public void onShutdownRequest() {
		applicationMaster.setDone(true);
		
	}

	
	
	
}
