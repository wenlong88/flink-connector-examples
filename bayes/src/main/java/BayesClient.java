/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aliyuncs.AcsRequest;
import com.aliyuncs.AcsResponse;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.exceptions.ServerException;
import com.aliyuncs.foas.model.v20181111.GetInstanceRunSummaryRequest;
import com.aliyuncs.foas.model.v20181111.GetInstanceRunSummaryResponse;
import com.aliyuncs.foas.model.v20181111.GetJobRequest;
import com.aliyuncs.foas.model.v20181111.GetJobResponse;
import com.aliyuncs.foas.model.v20181111.ListChildFolderRequest;
import com.aliyuncs.foas.model.v20181111.ListChildFolderResponse;
import com.aliyuncs.foas.model.v20181111.ListJobRequest;
import com.aliyuncs.foas.model.v20181111.ModifyInstanceStateRequest;
import com.aliyuncs.foas.model.v20181111.ModifyInstanceStateResponse;
import com.aliyuncs.foas.model.v20181111.StartJobRequest;
import com.aliyuncs.foas.model.v20181111.StartJobResponse;
import com.aliyuncs.foas.model.v20181111.ValidateJobRequest;
import com.aliyuncs.foas.model.v20181111.ValidateJobResponse;
import com.aliyuncs.http.FormatType;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import com.google.gson.Gson;

public class BayesClient {

	public BayesClient(String accessId, String accessKey, String project, String userId, String envId) {
		//云账号ak，更多信息请参考[bayes sdk用户群]的群公告
		//集团内需要将云账号与project绑定起来，才能操作project，如果没有绑定的话，在sdk用户群里提供相应信息给群主绑定
		IClientProfile profile = DefaultProfile.getProfile(regionId, accessId, accessKey);
		client = new DefaultAcsClient(profile);
		client.setAutoRetry(false);//不重试

		this.projectName = project;
		this.envId = envId;
		this.userId = userId;
	}

	private String envId;
	private String userId;
	private  DefaultAcsClient client;

	//集团内regionId统一填center，不要修改
	private static final String regionId = "center";

	private String projectName = "your project name";

	private <T extends AcsResponse> T getResponse(AcsRequest<T> request) {
		AcsResponse response = null;
		try {

			//cn-hangzhou-internal      集团生产
			//cn-hangzhou-internal-pre  集团预发
			request.putHeaderParameter("RegionId", envId);//根据自己的需要填写，必填
			request.putHeaderParameter("x-acs-bayes-user", userId);//六位工号，根据实际情况填写，必填

			//必须设置为json
			request.setHttpContentType(FormatType.JSON);
			request.setAcceptFormat(FormatType.JSON);

			response = client.getAcsResponse(request);
		} catch (Exception e) {
			//处理自己的异常逻辑，请注意异常中的requestId字段，排查问题时，需要提供该值给服务端
			if (e instanceof ServerException) {
				ServerException serverException = (ServerException) e;
				System.out.println(serverException.getRequestId());//本次请求的requestId
				System.out.println(serverException.getErrCode());//本次失败的的错误码
			}
			System.out.println(e.getMessage());
		}
		return (T) response;
	}

	public List<String> listFolder(String folder) {
		//获取文件夹列表
		ListChildFolderRequest listChildFolderRequest = new ListChildFolderRequest();
		listChildFolderRequest.setPath(folder); //根目录
		listChildFolderRequest.setProjectName(projectName);
		ListChildFolderResponse listChildFolderResponse = getResponse(listChildFolderRequest);
		System.out.println(new Gson().toJson(listChildFolderResponse));
		return listChildFolderResponse.getFolders().stream().map(fd -> fd.getPath()).collect(Collectors.toList());
	}

	public ValidateJobResponse validateJob(String jobName) {
		//校验job是否存在语法错误
		ValidateJobRequest validateJobRequest = new ValidateJobRequest();
		validateJobRequest.setProjectName(projectName);
		validateJobRequest.setJobName(jobName);
		ValidateJobResponse validateJobResponse = getResponse(validateJobRequest);
		return validateJobResponse;
	}

	public void pauseJob(String jobName) {
		//暂停作业，实例状态机参考文档
		GetInstanceRunSummaryRequest getInstanceRunSummaryRequest = new GetInstanceRunSummaryRequest();
		getInstanceRunSummaryRequest.setProjectName(projectName);
		getInstanceRunSummaryRequest.setJobName(jobName);
		getInstanceRunSummaryRequest.setInstanceId(-1L);
		GetInstanceRunSummaryResponse getInstanceRunSummaryResponse = getResponse(getInstanceRunSummaryRequest);

		if ("RUNNING".equals(getInstanceRunSummaryResponse.getRunSummary().getActualState())) {
			ModifyInstanceStateRequest modifyInstanceStateRequest = new ModifyInstanceStateRequest();
			modifyInstanceStateRequest.setProjectName(projectName);
			modifyInstanceStateRequest.setJobName(jobName);
			modifyInstanceStateRequest.setInstanceId(-1L);
			modifyInstanceStateRequest.setExpectState("PAUSED");
			ModifyInstanceStateResponse modifyInstanceStateResponse = getResponse(modifyInstanceStateRequest);
			System.out.println(new Gson().toJson(modifyInstanceStateResponse));

			//等待暂停成功
			while (true) {
				getInstanceRunSummaryRequest = new GetInstanceRunSummaryRequest();
				getInstanceRunSummaryRequest.setProjectName(projectName);
				getInstanceRunSummaryRequest.setJobName(jobName);
				getInstanceRunSummaryRequest.setInstanceId(-1L);
				getInstanceRunSummaryResponse = getResponse(getInstanceRunSummaryRequest);
				System.out.println(new Gson().toJson(getInstanceRunSummaryResponse));

				if (! "PAUSED".equals(getInstanceRunSummaryResponse.getRunSummary().getExpectState())) {
					modifyInstanceStateResponse = getResponse(modifyInstanceStateRequest);
					System.out.println(new Gson().toJson(modifyInstanceStateResponse));
				}

				if ("RUNNING".equals(getInstanceRunSummaryResponse.getRunSummary().getActualState())) {
					System.out.println(String.format("lastErrorTime[%s], lastErrorMessage[%s]",
							getInstanceRunSummaryResponse.getRunSummary().getLastErrorTime(),
							getInstanceRunSummaryResponse.getRunSummary().getLastErrorMessage()));
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
					}
				} else {
					break;
				}
			}
		}
	}

	public void resumeJob(String jobName) {
		//恢复作业，实例状态机参考文档
		GetInstanceRunSummaryRequest getInstanceRunSummaryRequest = new GetInstanceRunSummaryRequest();
		getInstanceRunSummaryRequest.setProjectName(projectName);
		getInstanceRunSummaryRequest.setJobName(jobName);
		getInstanceRunSummaryRequest.setInstanceId(-1L);
		GetInstanceRunSummaryResponse getInstanceRunSummaryResponse = getResponse(getInstanceRunSummaryRequest);

		if ("PAUSED".equals(getInstanceRunSummaryResponse.getRunSummary().getActualState())) {
			ModifyInstanceStateRequest modifyInstanceStateRequest = new ModifyInstanceStateRequest();
			modifyInstanceStateRequest.setProjectName(projectName);
			modifyInstanceStateRequest.setJobName(jobName);
			modifyInstanceStateRequest.setInstanceId(-1L);
			modifyInstanceStateRequest.setExpectState("RUNNING");
			ModifyInstanceStateResponse modifyInstanceStateResponse = getResponse(modifyInstanceStateRequest);
			System.out.println(new Gson().toJson(modifyInstanceStateResponse));

			//等待恢复成功
			while (true) {
				getInstanceRunSummaryRequest = new GetInstanceRunSummaryRequest();
				getInstanceRunSummaryRequest.setProjectName(projectName);
				getInstanceRunSummaryRequest.setJobName(jobName);
				getInstanceRunSummaryRequest.setInstanceId(-1L);
				getInstanceRunSummaryResponse = getResponse(getInstanceRunSummaryRequest);
				System.out.println(new Gson().toJson(getInstanceRunSummaryResponse));

				if (! "RUNNING".equals(getInstanceRunSummaryResponse.getRunSummary().getExpectState())) {
					modifyInstanceStateResponse = getResponse(modifyInstanceStateRequest);
					System.out.println(new Gson().toJson(modifyInstanceStateResponse));
				}
				if ("PAUSED".equals(getInstanceRunSummaryResponse.getRunSummary().getActualState())) {
					System.out.println(String.format("lastErrorTime[%s], lastErrorMessage[%s]",
							getInstanceRunSummaryResponse.getRunSummary().getLastErrorTime(),
							getInstanceRunSummaryResponse.getRunSummary().getLastErrorMessage()));
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
					}
				} else {
					break;
				}
			}
		}
	}

	public void stopJob(String jobName) {
		//停止作业，实例状态机参考文档
		GetInstanceRunSummaryRequest getInstanceRunSummaryRequest = new GetInstanceRunSummaryRequest();
		getInstanceRunSummaryRequest.setProjectName(projectName);
		getInstanceRunSummaryRequest.setJobName(jobName);
		getInstanceRunSummaryRequest.setInstanceId(-1L);
		GetInstanceRunSummaryResponse getInstanceRunSummaryResponse = getResponse(getInstanceRunSummaryRequest);
		System.out.println(new Gson().toJson(getInstanceRunSummaryResponse));

		if ("RUNNING".equals(getInstanceRunSummaryResponse.getRunSummary().getActualState())) {
			ModifyInstanceStateRequest modifyInstanceStateRequest = new ModifyInstanceStateRequest();
			modifyInstanceStateRequest.setProjectName(projectName);
			modifyInstanceStateRequest.setJobName(jobName);
			modifyInstanceStateRequest.setInstanceId(-1L);
			modifyInstanceStateRequest.setExpectState("TERMINATED");
			ModifyInstanceStateResponse modifyInstanceStateResponse = getResponse(modifyInstanceStateRequest);
			System.out.println(new Gson().toJson(modifyInstanceStateResponse));

			//等待停止成功
			while (true) {
				getInstanceRunSummaryRequest = new GetInstanceRunSummaryRequest();
				getInstanceRunSummaryRequest.setProjectName(projectName);
				getInstanceRunSummaryRequest.setJobName(jobName);
				getInstanceRunSummaryRequest.setInstanceId(-1L);
				getInstanceRunSummaryResponse = getResponse(getInstanceRunSummaryRequest);

				if ("RUNNING".equals(getInstanceRunSummaryResponse.getRunSummary().getActualState())) {
					System.out.println(String.format("lastErrorTime[%s], lastErrorMessage[%s]",
							getInstanceRunSummaryResponse.getRunSummary().getLastErrorTime(),
							getInstanceRunSummaryResponse.getRunSummary().getLastErrorMessage()));
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
					}
				} else {
					break;
				}
			}
		}
	}

	public void startJob(String jobName, long startOffset) {
		GetInstanceRunSummaryRequest getInstanceRunSummaryRequest = new GetInstanceRunSummaryRequest();
		getInstanceRunSummaryRequest.setProjectName(projectName);
		getInstanceRunSummaryRequest.setJobName(jobName);
		getInstanceRunSummaryRequest.setInstanceId(-1L);
		GetInstanceRunSummaryResponse getInstanceRunSummaryResponse = getResponse(getInstanceRunSummaryRequest);

		//启动作业，实例状态机参考文档
		if ("TERMINATED".equals(getInstanceRunSummaryResponse.getRunSummary().getActualState())) {
			StartJobRequest startJobRequest = new StartJobRequest();
			startJobRequest.setProjectName(projectName);
			startJobRequest.setJobName(jobName);

			Map map = new HashMap<String, String>();
			//startOffset表示启动点位
			map.put("startOffset", String.valueOf(startOffset));
			startJobRequest.setParameterJson(new Gson().toJson(map));//json格式参数
			StartJobResponse startJobResponse = getResponse(startJobRequest);
			System.out.println(new Gson().toJson(startJobResponse));

			//等待启动成功
			while (true) {
				getInstanceRunSummaryRequest = new GetInstanceRunSummaryRequest();
				getInstanceRunSummaryRequest.setProjectName(projectName);
				getInstanceRunSummaryRequest.setJobName(jobName);
				//-1表示获取当前实例
				getInstanceRunSummaryRequest.setInstanceId(-1L);
				getInstanceRunSummaryResponse = getResponse(getInstanceRunSummaryRequest);

				if ("WAITING".equals(getInstanceRunSummaryResponse.getRunSummary().getActualState())) {
					System.out.println(String.format("lastErrorTime[%s], lastErrorMessage[%s]",
							getInstanceRunSummaryResponse.getRunSummary().getLastErrorTime(),
							getInstanceRunSummaryResponse.getRunSummary().getLastErrorMessage()));
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
					}
				} else {
					break;
				}
			}
		}
	}

	public GetJobResponse.Job getJob(String jobName) {
		//获取作业
		GetJobRequest getJobRequest = new GetJobRequest();
		getJobRequest.setProjectName(projectName);
		getJobRequest.setJobName(jobName);
		GetJobResponse getJobResponse = getResponse(getJobRequest);
		System.out.println(new Gson().toJson(getJobResponse));
		return getJobResponse == null ? null : getJobResponse.getJob();
	}

	public static void main(String[] args) throws IOException {
		BayesClient client = new BayesClient("LTAICrVm6WLdnDIO", "yll2AmqLPQcK4a7H55Jz4uh1bY9IXz", "bayes_team", "065149", "cn-hangzhou-internal-pre");
		// client.pauseJob("ut_spm_simplifiy_1");
		String[] jobList = AutoScaleResourceCompare.getResourceContent("jobs").split("\n");
		for (String jobAndProject : jobList) {
			String[] items = jobAndProject.split(",");
			String jobName = items[1];
			if (client.getJob(jobName) != null) {
				client.pauseJob(jobName);
				client.resumeJob(jobName);
			}
		}
	}
}