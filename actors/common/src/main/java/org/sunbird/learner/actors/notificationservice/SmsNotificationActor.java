package org.sunbird.learner.actors.notificationservice;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.sunbird.actor.background.BackgroundOperations;
import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import org.sunbird.common.models.util.ProjectUtil.EsIndex;
import org.sunbird.common.models.util.ProjectUtil.EsType;
import org.sunbird.common.models.util.datasecurity.DecryptionService;
import org.sunbird.common.models.util.datasecurity.EncryptionService;
import org.sunbird.common.models.util.mail.SendMail;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.dto.SearchDTO;
import org.sunbird.helper.ServiceFactory;
import org.sunbird.learner.util.Util;
import org.sunbird.notification.sms.provider.ISmsProvider;
import org.sunbird.notification.utils.SMSFactory;

@ActorConfig(tasks = { "smsService" }, asyncTasks = { "smsService" })
public class SmsNotificationActor extends BaseActor {

    private CassandraOperation cassandraOperation = ServiceFactory.getInstance();
    private DecryptionService decryptionService = org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
            .getDecryptionServiceInstance(null);
    private EncryptionService encryptionService = org.sunbird.common.models.util.datasecurity.impl.ServiceFactory
            .getEncryptionServiceInstance(null);

    @Override
    public void onReceive(Request request) throws Throwable {
        if (request.getOperation().equalsIgnoreCase(BackgroundOperations.smsService.name())) {
            sendSms(request);
        } else {
            onReceiveUnsupportedOperation(request.getOperation());
        }
    }

    @SuppressWarnings({ "unchecked" })
    private void sendSms(Request actorMessage) {
        Map<String, Object> request = (Map<String, Object>) actorMessage.getRequest().get(JsonKey.SMS_REQUEST);

        List<String> userIds = (List<String>) request.get(JsonKey.RECIPIENT_USERIDS);
        String messageBody = (String) request.get(JsonKey.BODY);
        if (CollectionUtils.isEmpty(userIds)) {
            userIds = new ArrayList<>();
        }
        List<Map<String, Object>> users = this.getUsersFromDB(userIds);
        System.out.println("users ==========>");
        System.out.print(users);

        Response res = new Response();
        res.put(JsonKey.RESPONSE, JsonKey.SUCCESS);
        sender().tell(res, self());

        try {
            if (CollectionUtils.isNotEmpty(users)) {
                Iterator<Map<String, Object>> itr = users.iterator();
                while (itr.hasNext()) {
                    Map<String, Object> data = itr.next();
                    String phoneNo = decryptionService.decryptData((String) data.get(JsonKey.PHONE));
                    String countryCode = "";
                    if (StringUtils.isBlank((String) data.get(JsonKey.COUNTRY_CODE))) {
                        countryCode = ProjectUtil.getConfigValue(JsonKey.SUNBIRD_DEFAULT_COUNTRY_CODE);
                    } else {
                        countryCode = (String) data.get(JsonKey.COUNTRY_CODE);
                    }
                    ISmsProvider smsProvider = SMSFactory.getInstance("91SMS");

                    ProjectLogger.log(
                            "SmsNotificationActor:sendSms: SMS text = " + messageBody + " with phone = " + phoneNo,
                            LoggerEnum.INFO.name());

                    boolean response = smsProvider.send(phoneNo, countryCode, messageBody);

                    ProjectLogger.log("SmsNotificationActor:sendSms: Response from SMS provider: " + response,
                            LoggerEnum.INFO);

                    if (response) {
                        ProjectLogger.log("SmsNotificationActor:sendSms: SMS sent successfully to " + phoneNo,
                                LoggerEnum.INFO.name());
                    } else {
                        ProjectLogger.log("SmsNotificationActor:sendSms:  SMS send failed for " + phoneNo,
                                LoggerEnum.INFO.name());
                    }
                }
            }

        } catch (Exception e) {
            ProjectLogger.log("EmailServiceActor:sendMail: Exception occurred with message = " + e.getMessage(), e);
        }
    }

    private List<Map<String, Object>> getUsersFromDB(List<String> userIds) {
        Util.DbInfo usrDbInfo = Util.dbInfoMap.get(JsonKey.USER_DB);
        List<String> userIdList = new ArrayList<>(userIds);
        List<String> fields = new ArrayList<>();
        fields.add(JsonKey.ID);
        fields.add(JsonKey.FIRST_NAME);
        fields.add(JsonKey.COUNTRY_CODE);
        fields.add(JsonKey.PHONE);
        fields.add(JsonKey.PHONE_VERIFIED);
        Response response = cassandraOperation.getRecordsByIdsWithSpecifiedColumns(usrDbInfo.getKeySpace(),
                usrDbInfo.getTableName(), fields, userIdList);
        List<Map<String, Object>> userList = (List<Map<String, Object>>) response.get(JsonKey.RESPONSE);
        return userList;
    }

}
