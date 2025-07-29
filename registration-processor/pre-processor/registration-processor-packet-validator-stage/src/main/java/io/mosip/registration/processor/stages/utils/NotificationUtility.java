package io.mosip.registration.processor.stages.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.kernel.core.util.DateUtils;
import io.mosip.kernel.core.util.exception.JsonProcessingException;
import io.mosip.registration.processor.core.code.ApiName;
import io.mosip.registration.processor.core.constant.LoggerFileConstant;
import io.mosip.registration.processor.core.constant.MappingJsonConstants;
import io.mosip.registration.processor.core.constant.ProviderStageName;
import io.mosip.registration.processor.core.exception.ApisResourceAccessException;
import io.mosip.registration.processor.core.exception.PacketManagerException;
import io.mosip.registration.processor.core.exception.TemplateProcessingFailureException;
import io.mosip.registration.processor.core.exception.util.PlatformErrorMessages;
import io.mosip.registration.processor.core.exception.util.PlatformSuccessMessages;
import io.mosip.registration.processor.core.http.RequestWrapper;
import io.mosip.registration.processor.core.http.ResponseWrapper;
import io.mosip.registration.processor.core.logger.LogDescription;
import io.mosip.registration.processor.core.logger.RegProcessorLogger;
import io.mosip.registration.processor.core.notification.template.generator.dto.ResponseDto;
import io.mosip.registration.processor.core.notification.template.generator.dto.SmsRequestDto;
import io.mosip.registration.processor.core.notification.template.generator.dto.SmsResponseDto;
import io.mosip.registration.processor.core.spi.restclient.RegistrationProcessorRestClientService;
import io.mosip.registration.processor.core.status.util.StatusUtil;
import io.mosip.registration.processor.core.util.JsonUtil;
import io.mosip.registration.processor.core.util.LanguageUtility;
import io.mosip.registration.processor.message.sender.exception.TemplateGenerationFailedException;
import io.mosip.registration.processor.message.sender.exception.TemplateNotFoundException;
import io.mosip.registration.processor.message.sender.template.TemplateGenerator;
import io.mosip.registration.processor.packet.storage.utils.PriorityBasedPacketManagerService;
import io.mosip.registration.processor.packet.storage.utils.Utilities;
import io.mosip.registration.processor.rest.client.utils.RestApiClient;
import io.mosip.registration.processor.stages.dto.MessageSenderDTO;
import io.mosip.registration.processor.status.dto.InternalRegistrationStatusDto;
import io.mosip.registration.processor.status.dto.RegistrationAdditionalInfoDTO;
import io.mosip.registration.processor.status.dto.SyncTypeDto;
import io.mosip.registration.processor.status.entity.SyncRegistrationEntity;

@Component
public class NotificationUtility {

	/** The reg proc logger. */
	private static final Logger regProcLogger = RegProcessorLogger.getLogger(NotificationUtility.class);

	/** The Constant BOTH. */
	public static final String BOTH = "BOTH";

	/** The Constant LINE_SEPARATOR. */
	public static final String LINE_SEPARATOR = "" + '\n' + '\n' + '\n';

	/** The Constant DATA_SEPARATOR. */
	public static final String DATA_SEPARATOR = "_";

	/** The Constant FILE_SEPARATOR. */
	public static final String FILE_SEPARATOR = File.separator;

	/** The Constant ENCODING. */
	public static final String ENCODING = "UTF-8";

	@Autowired
	private RegistrationProcessorRestClientService<Object> restClientService;

	private String registrationId = null;

	/** The primary language. */
	@Value("${mosip.default.template-languages:#{null}}")
	private String defaultTemplateLanguages;

	@Value("${mosip.notification.language-type}")
	private String languageType;

	@Value("${mosip.default.user-preferred-language-attribute:#{null}}")
	private String userPreferredLanguageAttribute;
	/** The env. */
	@Autowired
	private Environment env;

	/** The template generator. */
	@Autowired
	private TemplateGenerator templateGenerator;

	@Autowired
	private LanguageUtility languageUtility;

	/** The resclient. */
	@Autowired
	private RestApiClient resclient;

	@Autowired
	private PriorityBasedPacketManagerService packetManagerService;

	/** The utility. */
	@Autowired
	private Utilities utility;

	private static final String SMS_SERVICE_ID = "mosip.registration.processor.sms.id";
	private static final String REG_PROC_APPLICATION_VERSION = "mosip.registration.processor.application.version";
	private static final String DATETIME_PATTERN = "mosip.registration.processor.datetime.pattern";
	private static final String NOTIFICATION_TEMPLATE_CODE = "regproc.packet.validator.notification.template.code.";
	private static final String EMAIL = "email";
	private static final String SMS = "sms";
	private static final String SUB = "sub";
	private static final String NEW_REG = NOTIFICATION_TEMPLATE_CODE + "new.reg.";
	private static final String LOST_UIN = NOTIFICATION_TEMPLATE_CODE + "lost.uin.";
	private static final String REPRINT_UIN = NOTIFICATION_TEMPLATE_CODE + "reprint.uin.";
	private static final String ACTIVATE = NOTIFICATION_TEMPLATE_CODE + "activate.";
	private static final String DEACTIVATE = NOTIFICATION_TEMPLATE_CODE + "deactivate.";
	private static final String UIN_UPDATE = NOTIFICATION_TEMPLATE_CODE + "uin.update.";
	private static final String RES_UPDATE = NOTIFICATION_TEMPLATE_CODE + "resident.update.";
	private static final String TECHNICAL_ISSUE = NOTIFICATION_TEMPLATE_CODE + "technical.issue.";
	private static final String SUP_REJECT = NOTIFICATION_TEMPLATE_CODE + "supervisor.reject.";

	@Autowired
	private ObjectMapper mapper;

	private static final Map<String, NotificationTemplateType> TEMPLATE_TYPE_MAP = Map.of(
	        SyncTypeDto.LOST.getValue().toUpperCase(), NotificationTemplateType.LOST_UIN,
	        SyncTypeDto.NEW.getValue().toUpperCase(), NotificationTemplateType.NEW_REG,
	        SyncTypeDto.UPDATE.getValue().toUpperCase(), NotificationTemplateType.UIN_UPDATE,
	        SyncTypeDto.RES_REPRINT.getValue().toUpperCase(), NotificationTemplateType.REPRINT_UIN,
	        SyncTypeDto.ACTIVATED.getValue().toUpperCase(), NotificationTemplateType.ACTIVATE,
	        SyncTypeDto.DEACTIVATED.getValue().toUpperCase(), NotificationTemplateType.DEACTIVATE,
	        SyncTypeDto.RES_UPDATE.getValue().toUpperCase(), NotificationTemplateType.RES_UPDATE
	);
	
	private static final Map<NotificationTemplateType, String> TEMPLATE_PREFIX_MAP = Map.of(
	        NotificationTemplateType.NEW_REG, "NEW_REG",
	        NotificationTemplateType.LOST_UIN, "LOST_UIN",
	        NotificationTemplateType.UIN_UPDATE, "UIN_UPDATE",
	        NotificationTemplateType.REPRINT_UIN, "REPRINT_UIN",
	        NotificationTemplateType.ACTIVATE, "ACTIVATE",
	        NotificationTemplateType.DEACTIVATE, "DEACTIVATE",
	        NotificationTemplateType.RES_UPDATE, "RES_UPDATE",
	        NotificationTemplateType.TECHNICAL_ISSUE, "TECHNICAL_ISSUE",
	        NotificationTemplateType.SUP_REJECT, "SUP_REJECT"
	);
	
	public void sendNotification(RegistrationAdditionalInfoDTO registrationAdditionalInfoDTO,
			InternalRegistrationStatusDto registrationStatusDto, SyncRegistrationEntity regEntity,
			String[] allNotificationTypes, boolean isProcessingSuccess, boolean isValidSupervisorStatus)
			throws ApisResourceAccessException, IOException, PacketManagerException, JsonProcessingException,
			JSONException {

		registrationId = regEntity.getRegistrationId();
		LogDescription description = new LogDescription();
		String regType = regEntity.getRegistrationType();
		MessageSenderDTO messageSenderDTO = new MessageSenderDTO();
		Map<String, Object> attributes = new HashMap<>();
		attributes.put("RID", registrationId);

		List<String> preferredLanguages = getPreferredLanguages(registrationStatusDto);
		JSONObject regProcessorIdentityJson = utility
				.getRegistrationProcessorMappingJson(MappingJsonConstants.IDENTITY);
		String nameField = JsonUtil.getJSONValue(
				JsonUtil.getJSONObject(regProcessorIdentityJson, MappingJsonConstants.NAME),
				MappingJsonConstants.VALUE);
		String[] nameArray = nameField.toString().split(",");

		// Pre-determine notification type only once
		NotificationTemplateType type;
		if (isProcessingSuccess) {
			type = setNotificationTemplateType(registrationStatusDto, null);
		} else if (!isValidSupervisorStatus) {
			type = NotificationTemplateType.SUP_REJECT;
		} else {
			type = NotificationTemplateType.TECHNICAL_ISSUE;
		}

		if (type != null) {
			setTemplateAndSubject(type, regType, messageSenderDTO);
		}

		boolean hasEmail = registrationAdditionalInfoDTO.getEmail() != null
				&& !registrationAdditionalInfoDTO.getEmail().isEmpty();
		boolean hasPhone = registrationAdditionalInfoDTO.getPhone() != null
				&& !registrationAdditionalInfoDTO.getPhone().isEmpty();

		// Skip processing entirely if no notification types match
		if (allNotificationTypes == null || (!hasEmail && !hasPhone)) {
			return;
		}

		// Process each language
		for (String preferredLanguage : preferredLanguages) {

			// Populate attributes only once per language
			String nameValue = registrationAdditionalInfoDTO.getName() != null ? registrationAdditionalInfoDTO.getName()
					: "";
			attributes.put(nameArray[0] + DATA_SEPARATOR + preferredLanguage, nameValue);

			for (int i = 1; i < nameArray.length; i++) {
				attributes.put(nameArray[i] + DATA_SEPARATOR + preferredLanguage, "");
			}

			// Send notifications
			for (String notificationType : allNotificationTypes) {
				if (EMAIL.equalsIgnoreCase(notificationType)) {
					if (hasEmail) {
						sendEmailNotification(registrationAdditionalInfoDTO, messageSenderDTO, attributes, description,
								preferredLanguage);
					}
				} else if (SMS.equalsIgnoreCase(notificationType)) {
					if (hasPhone) {
						sendSMSNotification(registrationAdditionalInfoDTO, messageSenderDTO, attributes, description,
								preferredLanguage);
					}
				}
			}
		}
	}

	private List<String> getPreferredLanguages(InternalRegistrationStatusDto registrationStatusDto)
			throws ApisResourceAccessException, PacketManagerException, JsonProcessingException, IOException,
			JSONException {
		if (userPreferredLanguageAttribute != null && !userPreferredLanguageAttribute.isBlank()) {
			try {
				String preferredLang = packetManagerService.getField(registrationStatusDto.getRegistrationId(),
						userPreferredLanguageAttribute, registrationStatusDto.getRegistrationType(),
						ProviderStageName.PACKET_VALIDATOR);

				if (preferredLang != null && !preferredLang.isBlank()) {
					List<String> codes = new ArrayList<>();
					for (String lang : preferredLang.split(",")) {
						String langCode = languageUtility.getLangCodeFromNativeName(lang);
						if (langCode != null && !langCode.isBlank())
							codes.add(langCode);
					}
					if (!codes.isEmpty())
						return codes;
				}
			} catch (ApisResourceAccessException e) {
				regProcLogger.error(LoggerFileConstant.SESSIONID.toString(),
						LoggerFileConstant.REGISTRATIONID.toString(), registrationStatusDto.getRegistrationId(),
						PlatformErrorMessages.RPR_PGS_API_RESOURCE_NOT_AVAILABLE.name() + e.getMessage()
								+ ExceptionUtils.getStackTrace(e));
			}
		}

		if (defaultTemplateLanguages != null && !defaultTemplateLanguages.isBlank()) {
			return List.of(defaultTemplateLanguages.split(","));
		}

		Map<String, String> idValuesMap = packetManagerService.getAllFieldsByMappingJsonKeys(
				registrationStatusDto.getRegistrationId(), registrationStatusDto.getRegistrationType(),
				ProviderStageName.PACKET_VALIDATOR);

		List<String> idValues = new ArrayList<>();
		for (Entry<String, String> entry : idValuesMap.entrySet()) {
			if (entry.getValue() != null && !entry.getValue().isBlank()) {
				idValues.add(entry.getValue());
			}
		}
		Set<String> langSet = new HashSet<>();
		for (String idValue : idValues) {
			if (idValue != null && !idValue.isBlank()) {
				if (isJSONArrayValid(idValue)) {
					org.json.simple.JSONArray array = mapper.readValue(idValue, org.json.simple.JSONArray.class);
					for (Object obj : array) {
						org.json.simple.JSONObject json = new org.json.simple.JSONObject((Map) obj);
						langSet.add((String) json.get("language"));
					}
				}
			}
		}
		return new ArrayList<>(langSet);
	}

	public boolean isJSONArrayValid(String jsonArrayString) {
		try {
			new JSONArray(jsonArrayString);
		} catch (JSONException ex) {
			return false;
		}
		return true;
	}

	private void sendSMSNotification(RegistrationAdditionalInfoDTO registrationAdditionalInfoDTO,
			MessageSenderDTO messageSenderDTO, Map<String, Object> attributes, LogDescription description,
			String preferedLanguage) {
		try {
			SmsResponseDto smsResponse = sendSMS(registrationAdditionalInfoDTO, messageSenderDTO.getSmsTemplateCode(),
					attributes, preferedLanguage);

			if (smsResponse.getStatus().equalsIgnoreCase("success")) {
				description.setCode(PlatformSuccessMessages.RPR_MESSAGE_SENDER_STAGE_SUCCESS.getCode());
				description.setMessage(StatusUtil.MESSAGE_SENDER_SMS_SUCCESS.getMessage());
				regProcLogger.info(LoggerFileConstant.SESSIONID.toString(),
						LoggerFileConstant.REGISTRATIONID.toString(), registrationId,
						description.getCode() + description.getMessage());
			} else {
				description.setCode(PlatformErrorMessages.RPR_MESSAGE_SENDER_SMS_FAILED.getCode());
				description.setMessage(StatusUtil.MESSAGE_SENDER_SMS_FAILED.getMessage());
				regProcLogger.info(LoggerFileConstant.SESSIONID.toString(),
						LoggerFileConstant.REGISTRATIONID.toString(), registrationId,
						description.getCode() + description.getMessage());
			}
		} catch (IOException | JSONException | ApisResourceAccessException e) {
			description.setCode(PlatformErrorMessages.RPR_MESSAGE_SENDER_SMS_FAILED.getCode());
			description.setMessage(StatusUtil.MESSAGE_SENDER_SMS_FAILED.getMessage());
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), description.getCode(), registrationId,
					description + e.getMessage() + ExceptionUtils.getStackTrace(e));
		}
	}

	private SmsResponseDto sendSMS(RegistrationAdditionalInfoDTO registrationAdditionalInfoDTO, String templateTypeCode,
			Map<String, Object> attributes, String preferredLanguage)
			throws ApisResourceAccessException, IOException, JSONException {

		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(),
				registrationId, "NotificationUtility::sendSms()::entry");
		try (InputStream in = templateGenerator.getTemplate(templateTypeCode, attributes, preferredLanguage)) {
			String artifact = IOUtils.toString(in, ENCODING);

			SmsRequestDto smsDto = new SmsRequestDto();
			smsDto.setNumber(registrationAdditionalInfoDTO.getPhone());
			smsDto.setMessage(artifact);

			RequestWrapper<SmsRequestDto> requestWrapper = new RequestWrapper<>();
			requestWrapper.setId(env.getProperty(SMS_SERVICE_ID));
			requestWrapper.setVersion(env.getProperty(REG_PROC_APPLICATION_VERSION));
			DateTimeFormatter format = DateTimeFormatter.ofPattern(env.getProperty(DATETIME_PATTERN));
			LocalDateTime localdatetime = LocalDateTime
					.parse(DateUtils.getUTCCurrentDateTimeString(env.getProperty(DATETIME_PATTERN)), format);
			requestWrapper.setRequesttime(localdatetime);
			requestWrapper.setRequest(smsDto);

			regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(),
					registrationId, "NotificationUtility::sendSms():: SMSNOTIFIER POST service started with request : "
							+ JsonUtil.objectMapperObjectToJson(requestWrapper));

			ResponseWrapper<?> responseWrapper = (ResponseWrapper<?>) restClientService.postApi(ApiName.SMSNOTIFIER, "",
					"", requestWrapper, ResponseWrapper.class);
			SmsResponseDto response = mapper.readValue(mapper.writeValueAsString(responseWrapper.getResponse()),
					SmsResponseDto.class);

			regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(),
					registrationId, "NotificationUtility::sendSms():: SMSNOTIFIER POST service ended with response : "
							+ JsonUtil.objectMapperObjectToJson(response));
			return response;
		} catch (TemplateNotFoundException | TemplateProcessingFailureException e) {
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					registrationId, PlatformErrorMessages.RPR_SMS_TEMPLATE_GENERATION_FAILURE.name() + e.getMessage()
							+ ExceptionUtils.getStackTrace(e));

			throw new TemplateGenerationFailedException(
					PlatformErrorMessages.RPR_SMS_TEMPLATE_GENERATION_FAILURE.getCode(), e);
		} catch (ApisResourceAccessException e) {
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					registrationId, PlatformErrorMessages.RPR_PGS_API_RESOURCE_NOT_AVAILABLE.name() + e.getMessage()
							+ ExceptionUtils.getStackTrace(e));

			throw new ApisResourceAccessException(PlatformErrorMessages.RPR_PGS_API_RESOURCE_NOT_AVAILABLE.name(), e);
		}
	}

	private void sendEmailNotification(RegistrationAdditionalInfoDTO registrationAdditionalInfoDTO,
			MessageSenderDTO messageSenderDTO, Map<String, Object> attributes, LogDescription description,
			String preferedLanguage) {
		try {
			String subjectTemplateCode = messageSenderDTO.getSubjectTemplateCode();

			ResponseDto emailResponse = sendEmail(registrationAdditionalInfoDTO,
					messageSenderDTO.getEmailTemplateCode(), subjectTemplateCode, attributes, preferedLanguage);
			if (emailResponse.getStatus().equalsIgnoreCase("success")) {
				description.setCode(PlatformSuccessMessages.RPR_MESSAGE_SENDER_STAGE_SUCCESS.getCode());
				description.setMessage(StatusUtil.MESSAGE_SENDER_EMAIL_SUCCESS.getMessage());

				regProcLogger.info(LoggerFileConstant.SESSIONID.toString(),
						LoggerFileConstant.REGISTRATIONID.toString(), registrationId,
						description.getCode() + description.getMessage());
			} else {
				description.setCode(PlatformErrorMessages.RPR_MESSAGE_SENDER_EMAIL_FAILED.getCode());
				description.setMessage(StatusUtil.MESSAGE_SENDER_EMAIL_FAILED.getMessage());

				regProcLogger.info(LoggerFileConstant.SESSIONID.toString(),
						LoggerFileConstant.REGISTRATIONID.toString(), registrationId,
						description.getCode() + description.getMessage());
			}
		} catch (Exception e) {
			description.setCode(PlatformErrorMessages.RPR_MESSAGE_SENDER_EMAIL_FAILED.getCode());
			description.setMessage(PlatformErrorMessages.RPR_MESSAGE_SENDER_EMAIL_FAILED.getMessage());

			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), description.getCode(), registrationId,
					description + e.getMessage() + ExceptionUtils.getStackTrace(e));
		}
	}

	private ResponseDto sendEmail(RegistrationAdditionalInfoDTO registrationAdditionalInfoDTO, String templateTypeCode,
			String subjectTypeCode, Map<String, Object> attributes, String preferredLanguage) throws Exception {

		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(),
				registrationId, "NotificationUtility::sendEmail()::entry");
		try (InputStream in = templateGenerator.getTemplate(templateTypeCode, attributes, preferredLanguage);
				InputStream subjectInputStream = templateGenerator.getTemplate(subjectTypeCode, attributes,
						preferredLanguage)) {
			String artifact = IOUtils.toString(in, ENCODING);
			String subjectArtifact = IOUtils.toString(subjectInputStream, ENCODING);

			String mailTo = registrationAdditionalInfoDTO.getEmail();

			ResponseDto response = sendEmail(mailTo, subjectArtifact, artifact);

			regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(),
					registrationId, "NotificationUtility::sendEmail()::exit");

			return response;
		} catch (TemplateNotFoundException | TemplateProcessingFailureException e) {
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					registrationId, PlatformErrorMessages.RPR_SMS_TEMPLATE_GENERATION_FAILURE.name() + e.getMessage()
							+ ExceptionUtils.getStackTrace(e));

			throw new TemplateGenerationFailedException(
					PlatformErrorMessages.RPR_SMS_TEMPLATE_GENERATION_FAILURE.getCode(), e);
		} catch (ApisResourceAccessException e) {
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					registrationId, PlatformErrorMessages.RPR_PGS_API_RESOURCE_NOT_AVAILABLE.name() + e.getMessage()
							+ ExceptionUtils.getStackTrace(e));

			throw new ApisResourceAccessException(PlatformErrorMessages.RPR_PGS_API_RESOURCE_NOT_AVAILABLE.name(), e);
		}
	}

	private ResponseDto sendEmail(String mailTo, String subjectArtifact, String artifact) throws Exception {		
		String apiHost = env.getProperty(ApiName.EMAILNOTIFIER.name());
		UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(apiHost);

		LinkedMultiValueMap<String, Object> params = new LinkedMultiValueMap<>();
		params.add("mailTo", mailTo);
		params.add("mailSubject", subjectArtifact);
		params.add("mailContent", artifact);
		params.add("attachments", null);

		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"NotificationUtility::sendEmail():: EMAILNOTIFIER POST service started");

		ResponseWrapper<?> responseWrapper = (ResponseWrapper<?>) resclient.postApi(builder.build().toUriString(),
				MediaType.MULTIPART_FORM_DATA, params, ResponseWrapper.class);

		ResponseDto responseDto = mapper.readValue(mapper.writeValueAsString(responseWrapper.getResponse()), ResponseDto.class);
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"NotificationUtility::sendEmail():: EMAILNOTIFIER POST service ended with in response : "
						+ JsonUtil.objectMapperObjectToJson(responseDto));

		return responseDto;
	}

	private NotificationTemplateType setNotificationTemplateType(InternalRegistrationStatusDto registrationStatusDto,
			NotificationTemplateType type) {
		if (registrationStatusDto == null || registrationStatusDto.getRegistrationType() == null) {
	        return type;
	    }

	    return TEMPLATE_TYPE_MAP.getOrDefault(
	            registrationStatusDto.getRegistrationType().toUpperCase(),
	            type
	    );
	}

	private void setTemplateAndSubject(NotificationTemplateType templateType, String regType,
			MessageSenderDTO messageSenderDTO) {
		String prefix = TEMPLATE_PREFIX_MAP.get(templateType);

	    if (prefix != null) {
	        messageSenderDTO.setSmsTemplateCode(env.getProperty(prefix + SMS));
	        messageSenderDTO.setEmailTemplateCode(env.getProperty(prefix + EMAIL));
	        messageSenderDTO.setSubjectTemplateCode(env.getProperty(prefix + SUB));
	    }
	}
}