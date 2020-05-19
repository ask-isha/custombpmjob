package appjob;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import org.apache.log4j.Logger;
import org.kie.api.executor.Command;
import org.kie.api.executor.CommandContext;
import org.kie.api.executor.ExecutionResults;
import org.kie.api.executor.Reoccurring;
import org.kie.server.api.marshalling.MarshallingFormat;
import org.kie.server.api.model.KieContainerResource;
import org.kie.server.api.model.KieContainerResourceList;
import org.kie.server.api.model.instance.TaskSummary;	
import org.kie.server.client.KieServicesClient;
import org.kie.server.client.KieServicesConfiguration;
import org.kie.server.client.KieServicesFactory;
import org.kie.server.client.UserTaskServicesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueueJob implements Command, Reoccurring {
	private static final Logger logger = LoggerFactory.getLogger(QueueJob.class);
	public Date getScheduleTime() {
		// TODO Auto-generated method stub
		int nextScheduleTimeAdd = 2;

		if (nextScheduleTimeAdd < 0) {
			return null;
		}
		Calendar calTime = Calendar.getInstance();

		Date nextSchedule = new Date();
		calTime.setTime(nextSchedule);
		calTime.add(Calendar.MINUTE, 1);
		logger.info("Next schedule for job {} is set to {}" + this.getClass().getSimpleName() + " " + calTime.getTime());
		//System.out.println(
			//	"Next schedule for job {} is set to {}" + this.getClass().getSimpleName() + " " + calTime.getTime());
		return calTime.getTime();

	}

	public ExecutionResults execute(CommandContext ctx) throws Exception {
		// TODO Auto-generated method stub
		String serverUrl = "http://localhost:8080/kie-server/services/rest/server";
		String user = "krisv"; //"sysAdmin"; // QueueConstants.USER
		String password = "krisv";
		String usrGrp = "L1";
		String tskStatus = "Ready";
		//String filterQueue="Pool-Queue-L1";
		List<String> userGrpLst = new ArrayList<String>();
		List<String> tskStatusLst = new ArrayList<String>();
		
		boolean overflowJobData = ctx.getData().containsKey("overflowJobData")
				? Boolean.parseBoolean((String) ctx.getData("overflowJobData"))
				: false;
		ExecutionResults executionResults = new ExecutionResults();
		executionResults.setData("overflowJobData", overflowJobData);
		try {

			logger.info("Job execution Started ---!");
			KieServicesConfiguration configuration = KieServicesFactory.newRestConfiguration(serverUrl, user, password);
			configuration.setMarshallingFormat(MarshallingFormat.JAXB);
			KieServicesClient kieServicesClient = KieServicesFactory.newKieServicesClient(configuration);
			KieContainerResourceList containers = kieServicesClient.listContainers().getResult();
			UserTaskServicesClient taskClient = kieServicesClient.getServicesClient(UserTaskServicesClient.class);
			Map<String, Object> params = new HashMap<String, Object>();

			if (containers != null) {

				for (KieContainerResource kieContainerResource : containers.getContainers()) {
					logger.info("Found container " + kieContainerResource.getContainerId());

					userGrpLst.add(usrGrp);
					tskStatusLst.add(tskStatus);
					List<TaskSummary> taskOwn = taskClient.findTasksAssignedAsPotentialOwner(user, userGrpLst, tskStatusLst, 0, 10,"",true);
					//taskClient.findTasksAssignedAsPotentialOwner(user, userGrpLst, tskStatusLst, 0, 10, "", true);
					//tskStatusLst, 0, 10);
					logger.info("Count of Task Available in L1 Pool Queue with Ready state" + taskOwn.size());

					for (int i = 0; i < taskOwn.size(); i++) {

						if (taskOwn.get(i).getContainerId().equalsIgnoreCase(kieContainerResource.getContainerId())) {
							logger.info("Task Id:::::::" + taskOwn.get(i).getId());
							taskClient.claimTask(kieContainerResource.getContainerId(), taskOwn.get(i).getId(), user);
							taskClient.startTask(kieContainerResource.getContainerId(), taskOwn.get(i).getId(), user);
							params.put("UserAction", "Overflow");
							taskClient.completeTask(kieContainerResource.getContainerId(), taskOwn.get(i).getId(), user,
									params);
							logger.info("Task Completed:::::::" );
						}

					}
				}
				logger.info("End Of container List:::::::" );
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}

		logger.info("Job execution Finished---!");

		return executionResults;
	}

}

