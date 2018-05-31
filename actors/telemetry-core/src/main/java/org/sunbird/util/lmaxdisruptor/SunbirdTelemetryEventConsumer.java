package org.sunbird.util.lmaxdisruptor;

import com.google.common.net.HttpHeaders;
import com.google.gson.Gson;
import com.lmax.disruptor.EventHandler;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.common.models.util.HttpUtil;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.request.Request;
import org.sunbird.common.request.TelemetryV3Request;

/**
 * Dispatcher for telemetry data to Sunbird telemetry service. Sunbird telemetry service is
 * responsible for storing telemetry data in Sunbird and/or Ekstep platform based on configuration.
 *
 * @author Manzarul
 */
public class SunbirdTelemetryEventConsumer implements EventHandler<Request> {

  @Override
  public void onEvent(Request request, long sequence, boolean endOfBatch) {
    ProjectLogger.log("SunbirdTelemetryEventConsumer: onEvent called.", LoggerEnum.INFO.name());
    if (request != null) {
      try {
        Gson gson = new Gson();
        String response =
            HttpUtil.sendPostRequest(
                getTelemetryUrl(), gson.toJson(getTelemetryRequest(request)), getHeaders());
        ProjectLogger.log(
            "SunbirdTelemetryEventConsumer:onEvent: Request process status = " + response,
            LoggerEnum.INFO.name());
      } catch (Exception e) {
        ProjectLogger.log(
            "SunbirdTelemetryEventConsumer:onEvent: Generic exception occurred in sending telemetry request = "
                + e.getMessage(),
            e);
        ProjectLogger.log(
            "SunbirdTelemetryEventConsumer:onEvent: Failure request = "
                + new Gson().toJson(getTelemetryRequest(request)),
            LoggerEnum.INFO.name());
      }
    }
  }

  private Map<String, String> getHeaders() {
    Map<String, String> headers = new HashMap<>();
    headers.put(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    return headers;
  }

  /**
   * This method will return telemetry url. it will read sunbird_lms_base_url key for base url .
   * First it will try to read value from environment in case of absence it will read value from
   * property cache.
   *
   * @return Complete url for telemetry service.
   */
  private String getTelemetryUrl() {
    ProjectUtil.getConfigValue(JsonKey.SUNBIRD_TELEMETRY_BASE_URL);
    String telemetryBaseUrl = System.getenv(JsonKey.SUNBIRD_TELEMETRY_BASE_URL);
    if (StringUtils.isBlank(telemetryBaseUrl)) {
      telemetryBaseUrl =
          PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_TELEMETRY_BASE_URL);
    }
    telemetryBaseUrl =
        telemetryBaseUrl
            + PropertiesCache.getInstance().getProperty(JsonKey.SUNBIRD_TELEMETRY_API_PATH);
    ProjectLogger.log(
        "SunbirdTelemetryEventConsumer:getTelemetryUrl: url = " + telemetryBaseUrl,
        LoggerEnum.INFO.name());
    return telemetryBaseUrl;
  }

  /**
   * This method will transform incoming requested data to Telemetry request structure.
   *
   * @param request Request that contains telemetry data generated by Sunbird.
   * @return Telemetry request structure.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private TelemetryV3Request getTelemetryRequest(Request request) {
    TelemetryV3Request telemetryV3Request = new TelemetryV3Request();
    if (request.getRequest().get(JsonKey.ETS) != null
        && request.getRequest().get(JsonKey.ETS) instanceof BigInteger) {
      telemetryV3Request.setEts(((BigInteger) request.getRequest().get(JsonKey.ETS)).longValue());
    }
    if (request.getRequest().get(JsonKey.EVENTS) != null
        && request.getRequest().get(JsonKey.EVENTS) instanceof List
        && !(((List) request.getRequest().get(JsonKey.EVENTS)).isEmpty())) {
      List<Map<String, Object>> events =
          (List<Map<String, Object>>) request.getRequest().get(JsonKey.EVENTS);
      telemetryV3Request.setEvents(events);
      ProjectLogger.log(
          "SunbirdTelemetryEventConsumer:getTelemetryRequest: Events count = " + events.size(),
          LoggerEnum.INFO.name());
    }
    return telemetryV3Request;
  }
}
