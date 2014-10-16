package ca.albertlockett.yarn;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync.CallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NMCallbackHandler implements CallbackHandler {

	private static final Logger log = 
			LoggerFactory.getLogger(NMCallbackHandler.class);
	
	private ConcurrentMap<ContainerId, Container> containers = 
			new ConcurrentHashMap<ContainerId, Container> ();
	private ApplicationMaster applicationMaster;
	
	
	public NMCallbackHandler(ApplicationMaster applicationMaster) {
		this.applicationMaster = applicationMaster;
	}
	
	public void addContainer(ContainerId containerId, Container container) {
		containers.putIfAbsent(containerId, container);
	}
	
	public void onContainerStarted(ContainerId containerId,
			Map<String, ByteBuffer> allServiceResponse) {
		log.debug("Suceeded to start Container {}", containerId);
		Container container = containers.get(containerId);
		if(container != null) {
			applicationMaster.getNmClient().getContainerStatusAsync(
					containerId,container.getNodeId());
		}
		
	}

	public void onContainerStatusReceived(ContainerId containerId,
			ContainerStatus containerStatus) {
		log.debug("Container status: id = {}, status = {}",
				containerId, containerStatus);
	}

	public void onContainerStopped(ContainerId containerId) {
		log.debug("Suceeded in stopping container: {}", containerId);
		containers.remove(containerId);
	}

	public void onGetContainerStatusError(
			ContainerId containerId, Throwable t) {
		log.error("Failed to query the status of Container {}", containerId);
		
	}

	public void onStartContainerError(ContainerId containerId, Throwable t) {
		log.error("Failed to start Container {}", containerId);
		containers.remove(containerId);
		applicationMaster.getNumcompletedContainers().incrementAndGet();
	}

	public void onStopContainerError(ContainerId containerId, Throwable t) {
		log.error("Failed to stop Container {}", containerId);
		containers.remove(containerId);
		
	}

}
