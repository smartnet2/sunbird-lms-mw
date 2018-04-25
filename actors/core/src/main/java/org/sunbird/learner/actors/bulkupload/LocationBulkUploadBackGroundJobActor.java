package org.sunbird.learner.actors.bulkupload;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.GeoLocationJsonKey;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LocationActorOperation;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.TelemetryEnvKey;
import org.sunbird.common.request.ExecutionContext;
import org.sunbird.common.request.Request;
import org.sunbird.learner.actors.bulkupload.dao.BulkUploadDao;
import org.sunbird.learner.actors.bulkupload.dao.impl.BulkUploadDaoImpl;
import org.sunbird.learner.actors.bulkupload.model.BulkUpload;
import org.sunbird.learner.actors.bulkupload.service.InterServiceCommunication;
import org.sunbird.learner.actors.bulkupload.service.InterServiceCommunicationFactory;
import org.sunbird.learner.util.Util;

/** Created by arvind on 24/4/18. */
@ActorConfig(
  tasks = {},
  asyncTasks = {"locationBulkUploadBackGround"}
)
public class LocationBulkUploadBackGroundJobActor extends BaseActor {

  BulkUploadDao bulkUploadDao = new BulkUploadDaoImpl();
  ObjectMapper mapper = new ObjectMapper();
  InterServiceCommunication interServiceCommunication =
      InterServiceCommunicationFactory.getInstance().getCommunicationPath("actorCommunication");

  @Override
  public void onReceive(Request request) throws Throwable {

    ProjectLogger.log(
        "LocationBulkUploadBackGroundJobActor  onReceive called", LoggerEnum.INFO.name());
    String operation = request.getOperation();

    Util.initializeContext(request, TelemetryEnvKey.GEO_LOCATION);
    ExecutionContext.setRequestId(request.getRequestId());

    switch (operation) {
      case "locationBulkUploadBackGround":
        bulkLocationUpload(request);
        break;
      default:
        onReceiveUnsupportedOperation("LocationBulkUploadBackGroundJobActor");
    }
  }

  private void bulkLocationUpload(Request request) {

    String processId = (String) request.get(JsonKey.PROCESS_ID);
    BulkUpload bulkUpload = bulkUploadDao.read(processId);
    if (null == bulkUpload) {
      // stop processing here ..
    }
    Integer status = bulkUpload.getStatus();
    if (!(status == (ProjectUtil.BulkProcessStatus.COMPLETED.getValue())
        || status == (ProjectUtil.BulkProcessStatus.INTERRUPT.getValue()))) {
      processLocationBulkUpoad(bulkUpload);
    }
  }

  private void processLocationBulkUpoad(BulkUpload bulkUpload) {

    TypeReference<List<Map<String, Object>>> mapType =
        new TypeReference<List<Map<String, Object>>>() {};
    List<Map<String, Object>> jsonList = new LinkedList<>();
    List<Map<String, Object>> successList = new LinkedList<>();
    List<Map<String, Object>> failureList = new LinkedList<>();
    try {
      jsonList = mapper.readValue(bulkUpload.getData(), mapType);
    } catch (IOException e) {
      // throw exception here and stop processing ...
      ProjectLogger.log(
          "Exception occurred while converting json String to List in BulkUploadBackGroundJobActor : ",
          e);
    }

    for (Map<String, Object> row : jsonList) {
      processLoc(row, bulkUpload, successList, failureList);
    }
  }

  private void processLoc(
      Map<String, Object> row,
      BulkUpload bulkUpload,
      List<Map<String, Object>> successList,
      List<Map<String, Object>> failureList) {

    if (row.containsKey(GeoLocationJsonKey.CODE)) {

      Request request = new Request();
      Map<String, Object> filters = new HashMap<>();
      filters.put(GeoLocationJsonKey.CODE, row.get(GeoLocationJsonKey.CODE));
      filters.put(GeoLocationJsonKey.CODE, row.get(GeoLocationJsonKey.LOCATION_TYPE));
      request.getRequest().put(JsonKey.FILTERS, filters);

      Object obj =
          interServiceCommunication.getResponse(
              request, LocationActorOperation.SEARCH_LOCATION.getValue());
      if (obj instanceof ProjectCommonException) {

      } else if (obj instanceof Response) {
        Response response = (Response) obj;
        List<Map<String, Object>> responseList =
            (List<Map<String, Object>>) response.getResult().get(JsonKey.RESPONSE);
        if (CollectionUtils.isEmpty(responseList)) {
          callCreateLocation(row, bulkUpload, successList, failureList);
        } else {
          callUpdateLocation(row, bulkUpload, successList, failureList, responseList.get(0));
        }
      }
    } else {

      // put in failures .. code does not exist so not valid request ..
    }
  }

  private void callUpdateLocation(
      Map<String, Object> row,
      BulkUpload bulkUpload,
      List<Map<String, Object>> successList,
      List<Map<String, Object>> failureList,
      Map<String, Object> response) {

    String id = (String) response.get(JsonKey.ID);
    row.put(JsonKey.ID, id);

    Request request = new Request();
    request.getRequest().putAll(row);
    Object obj =
        interServiceCommunication.getResponse(
            request, LocationActorOperation.UPDATE_LOCATION.getValue());

    if (obj instanceof ProjectCommonException) {

      failureList.add(row);
    } else if (obj instanceof Response) {
      successList.add(row);
    }
  }

  private void callCreateLocation(
      Map<String, Object> row,
      BulkUpload bulkUpload,
      List<Map<String, Object>> successList,
      List<Map<String, Object>> failureList) {

    Request request = new Request();
    request.getRequest().putAll(row);
    Object obj =
        interServiceCommunication.getResponse(
            request, LocationActorOperation.CREATE_LOCATION.getValue());

    if (obj instanceof ProjectCommonException) {
      failureList.add(row);
    } else if (obj instanceof Response) {
      successList.add(row);
    }
  }
}
