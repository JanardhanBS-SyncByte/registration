package io.mosip.registration.processor.packet.storage.utils;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.mosip.kernel.core.exception.ExceptionUtils;
import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.registration.processor.abis.queue.dto.AbisQueueDetails;
import io.mosip.registration.processor.core.code.ApiName;
import io.mosip.registration.processor.core.common.rest.dto.ErrorDTO;
import io.mosip.registration.processor.core.constant.LoggerFileConstant;
import io.mosip.registration.processor.core.exception.ApisResourceAccessException;
import io.mosip.registration.processor.core.exception.RegistrationProcessorCheckedException;
import io.mosip.registration.processor.core.exception.RegistrationProcessorUnCheckedException;
import io.mosip.registration.processor.core.exception.util.PlatformErrorMessages;
import io.mosip.registration.processor.core.idrepo.dto.IdResponseDTO1;
import io.mosip.registration.processor.core.idrepo.dto.ResponseDTO;
import io.mosip.registration.processor.core.logger.RegProcessorLogger;
import io.mosip.registration.processor.core.packet.dto.vid.VidResponseDTO;
import io.mosip.registration.processor.core.queue.factory.MosipQueue;
import io.mosip.registration.processor.core.spi.queue.MosipQueueConnectionFactory;
import io.mosip.registration.processor.core.spi.restclient.RegistrationProcessorRestClientService;
import io.mosip.registration.processor.core.util.JsonUtil;
import io.mosip.registration.processor.packet.manager.idreposervice.IdRepoService;
import io.mosip.registration.processor.packet.storage.dto.ConfigEnum;
import io.mosip.registration.processor.packet.storage.exception.IdRepoAppException;
import io.mosip.registration.processor.packet.storage.exception.QueueConnectionNotFound;
import io.mosip.registration.processor.packet.storage.exception.VidCreationException;
import io.mosip.registration.processor.rest.client.utils.RestApiClient;
import io.mosip.registration.processor.status.dao.RegistrationStatusDao;
import io.mosip.registration.processor.status.dto.InternalRegistrationStatusDto;
import io.mosip.registration.processor.status.entity.RegistrationStatusEntity;
import jakarta.annotation.PostConstruct;
import lombok.Data;

/**
 * The Class Utilities.
 *
 * @author Girish Yarru
 */
@Component

/**
 * Instantiates a new utilities.
 */
@Data
public class Utilities {
	/** The reg proc logger. */
	private static Logger regProcLogger = RegProcessorLogger.getLogger(Utilities.class);
	private static final String SOURCE = "source";
	private static final String PROCESS = "process";
	private static final String PROVIDER = "provider";

	/** The Constant UIN. */
	private static final String UIN = "UIN";

	/** The Constant FILE_SEPARATOR. */
	public static final String FILE_SEPARATOR = "\\";

	/** The Constant RE_PROCESSING. */
	private static final String RE_PROCESSING = "re-processing";

	/** The Constant HANDLER. */
	private static final String HANDLER = "handler";

	/** The Constant NEW_PACKET. */
	private static final String NEW_PACKET = "New-packet";

	/** The Constant INBOUNDQUEUENAME. */
	private static final String INBOUNDQUEUENAME = "inboundQueueName";

	/** The Constant OUTBOUNDQUEUENAME. */
	private static final String OUTBOUNDQUEUENAME = "outboundQueueName";

	/** The Constant ABIS. */
	private static final String ABIS = "abis";

	/** The Constant USERNAME. */
	private static final String USERNAME = "userName";

	/** The Constant PASSWORD. */
	private static final String PASSWORD = "password";

	/** The Constant BROKERURL. */
	private static final String BROKERURL = "brokerUrl";

	/** The Constant TYPEOFQUEUE. */
	private static final String TYPEOFQUEUE = "typeOfQueue";

	/** The Constant NAME. */
	private static final String NAME = "name";

	/** The Constant NAME. */
	private static final String INBOUNDMESSAGETTL = "inboundMessageTTL";

	/** The Constant FAIL_OVER. */
	private static final String FAIL_OVER = "failover:(";

	/** The Constant RANDOMIZE_FALSE. */
	private static final String RANDOMIZE_FALSE = ")?randomize=false";

	private static final String VALUE = "value";

	@Value("${mosip.kernel.machineid.length}")
	private int machineIdLength;

	@Value("${mosip.kernel.registrationcenterid.length}")
	private int centerIdLength;

	@Value("${provider.packetreader.mosip}")
	private String provider;

	/** The config server file storage URL. */
	@Value("${config.server.file.storage.uri}")
	private String configServerFileStorageURL;

	/** The get reg processor identity json. */
	@Value("${registration.processor.identityjson}")
	private String getRegProcessorIdentityJson;

	/** The get reg processor demographic identity. */
	@Value("${registration.processor.demographic.identity}")
	private String getRegProcessorDemographicIdentity;

	/** The get reg processor applicant type. */
	@Value("${registration.processor.applicant.type}")
	private String getRegProcessorApplicantType;

	/** The elapse time. */
	@Value("${registration.processor.reprocess.elapse.time}")
	private long elapseTime;

	/** The registration processor abis json. */
	@Value("${registration.processor.abis.json}")
	private String registrationProcessorAbisJson;

	/** The id repo update. */
	@Value("${registration.processor.id.repo.update}")
	private String idRepoUpdate;

	/** The vid version. */
	@Value("${registration.processor.id.repo.vidVersion}")
	private String vidVersion;

	@Value("#{'${registration.processor.queue.trusted.packages}'.split(',')}")
	private List<String> trustedPackages;

	private static final ConcurrentHashMap<String, JSONObject> jsonCache = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, String> sourceCache = new ConcurrentHashMap<>();
	private static final Cache<String, String> transactionIdCache = new Cache2kBuilder<String, String>() {
	}.expireAfterWrite(10, TimeUnit.MINUTES) // 10 minutes TTL
			.entryCapacity(10_000) // Maximum 10,000 entries
			.build();

	@Autowired
	private ObjectMapper objMapper;

	@Autowired
	private IdRepoService idRepoService;

	/** The rest client service. */
	@Autowired
	private RegistrationProcessorRestClientService<Object> restClientService;

	/** The mosip connection factory. */
	@Autowired
	private MosipQueueConnectionFactory<MosipQueue> mosipConnectionFactory;

	/** The registration status dao. */
	@Autowired
	private RegistrationStatusDao registrationStatusDao;

	private static Map<String, String> readerConfiguration;
	private static Map<String, String> writerConfiguration;

	@Autowired
	private RestApiClient restApiClient;

	private static RestApiClient staticRestApiClient;

	@PostConstruct
	public void initRestTemplate() {
		staticRestApiClient = restApiClient;
	}

	public static void initialize(Map<String, String> reader, Map<String, String> writer) {
		readerConfiguration = reader;
		writerConfiguration = writer;
	}

	/**
	 * Gets the json.
	 *
	 * @param configServerFileStorageURL the config server file storage URL
	 * @param uri                        the uri
	 * @return the json
	 */
	public static String getJson(String configServerFileStorageURL, String uri) throws IOException {
		if (staticRestApiClient == null) {
			throw new IllegalStateException("RestApiClient not initialized. Ensure Spring context is properly loaded.");
		}
		try {
			return staticRestApiClient.getApi(URI.create(configServerFileStorageURL + uri), String.class);
		} catch (Exception e) {
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.APPLICATIONID.toString(),
					LoggerFileConstant.APPLICATIONID.toString(),
					"Failed to fetch JSON from " + uri + ": " + ExceptionUtils.getStackTrace(e));
			throw new IOException("Failed to fetch JSON from " + uri, e);
		}
	}

	public String getDefaultSource(String process, ConfigEnum config) {
		Map<String, String> configMap = config.equals(ConfigEnum.READER) ? readerConfiguration : writerConfiguration;

		if (configMap != null) {
			for (Map.Entry<String, String> entry : configMap.entrySet()) {
				String[] values = entry.getValue().split(",");
				String source = null;
				for (String val : values) {
					if (val.startsWith(PROCESS + ":") && val.contains(process))
						for (String sVal : values) {
							if (sVal.startsWith(SOURCE + ":")) {
								source = sVal.replace(SOURCE + ":", "");
								return source;
							}
						}
				}
			}
		}
		return null;
	}

	public String getSource(String packetSegment, String process, String field) throws IOException {
        String cacheKey = packetSegment + "_" + process + "_" + (field != null ? field : "default");
        return sourceCache.computeIfAbsent(cacheKey, k -> {
            try {
                JSONObject jsonObject = getRegistrationProcessorMappingJson(packetSegment);
                Object obj = field == null ? jsonObject.get(PROVIDER) : ((Map) jsonObject.get(field)).get(PROVIDER);
                if (obj instanceof List) {
                    Set<String> providerSet = new HashSet<>((List<String>) obj);
                    for (String value : providerSet) {
                        String[] values = value.split(",");
                        for (String val : values) {
                            if (val.startsWith(PROCESS) && val.contains(process)) {
                                for (String v : values) {
                                    if (v.startsWith(SOURCE)) {
                                        return v.replace(SOURCE + ":", "").trim();
                                    }
                                }
                            }
                        }
                    }
                }
                return null;
            } catch (IOException e) {
                throw new RegistrationProcessorUnCheckedException(
                        PlatformErrorMessages.RPR_SYS_IO_EXCEPTION.getCode(),
                        PlatformErrorMessages.RPR_SYS_IO_EXCEPTION.getMessage(), e);
            }
        });
    }

	private Object getField(JSONObject jsonObject, String field) {
		LinkedHashMap<?, ?> lm = (LinkedHashMap<?, ?>) jsonObject.get(field);
		return lm.get(PROVIDER);
	}

	public String getSourceFromIdField(String packetSegment, String process, String idField) throws IOException {
		JSONObject jsonObject = getRegistrationProcessorMappingJson(packetSegment);
		for (Object key : jsonObject.keySet()) {
			LinkedHashMap<?, ?> hMap = (LinkedHashMap<?, ?>) jsonObject.get(key);
			String value = (String) hMap.get(VALUE);
			if (value != null && value.contains(idField)) {
				return getSource(packetSegment, process, key.toString());
			}
		}
		return null;
	}

	/**
	 * retrieving json from id repo by UIN.
	 *
	 * @param uin the uin
	 * @return the JSON object
	 * @throws ApisResourceAccessException the apis resource access exception
	 * @throws IdRepoAppException          the id repo app exception
	 * @throws IOException                 Signals that an I/O exception has
	 *                                     occurred.
	 */
	private ResponseDTO retrieveIdrepoResponseObj(String uin, String queryParam, String queryParamValue)
			throws ApisResourceAccessException {
		if (uin == null)
			return null;

		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
				"Utilities::retrieveIdrepoResponseObj()::entry");
		List<String> pathSegments = new ArrayList<>();
		pathSegments.add(uin);

		IdResponseDTO1 idResponseDto = (IdResponseDTO1) restClientService.getApi(ApiName.IDREPOGETIDBYUIN, pathSegments,
				queryParam == null ? "" : queryParam, queryParamValue == null ? "" : queryParamValue,
				IdResponseDTO1.class);

		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
				"Utilities::retrieveIdrepoDocument():: IDREPOGETIDBYUIN GET service call ended Successfully");

		if (idResponseDto == null) {
			regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
					"Utilities::retrieveIdrepoResponseObj()::exit idResponseDto is null");
			return null;
		}
		if (!idResponseDto.getErrors().isEmpty()) {
			List<ErrorDTO> error = idResponseDto.getErrors();
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
					"Utilities::retrieveIdrepoResponseObj():: error with error message " + error.get(0).getMessage());
			throw new IdRepoAppException(error.get(0).getMessage());
		}

		return idResponseDto.getResponse();
	}

	/**
	 * retrieving identity json ffrom id repo by UIN.
	 *
	 * @param uin the uin
	 * @return the JSON object
	 * @throws ApisResourceAccessException the apis resource access exception
	 * @throws IdRepoAppException          the id repo app exception
	 * @throws IOException                 Signals that an I/O exception has
	 *                                     occurred.
	 */
	public JSONObject retrieveIdrepoJson(String uin)
			throws ApisResourceAccessException, IdRepoAppException, IOException {
		if (uin == null)
			return null;

		ResponseDTO idResponseDto = retrieveIdrepoResponseObj(uin, null, null);
		if (idResponseDto != null) {
			regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
					"Utilities::retrieveIdrepoJson():: IDREPOGETIDBYUIN GET service call ended Successfully");
			try {
				String response = objMapper.writeValueAsString(idResponseDto.getIdentity());
				return (JSONObject) new JSONParser().parse(response);
			} catch (org.json.simple.parser.ParseException e) {
				regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
						ExceptionUtils.getStackTrace(e));
				throw new IdRepoAppException("Error while parsing string to JSONObject", e);
			}
		} else {
			regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
					"Utilities::retrieveIdrepoJson():: IDREPOGETIDBYUIN GET service Returned NULL");
		}
		return null;
	}

	/**
	 * retrieving identity json ffrom id repo by UIN.
	 *
	 * @param uin the uin
	 * @return the JSON object
	 * @throws ApisResourceAccessException the apis resource access exception
	 * @throws IdRepoAppException          the id repo app exception
	 * @throws IOException                 Signals that an I/O exception has
	 *                                     occurred.
	 */
	public List<io.mosip.registration.processor.core.idrepo.dto.Documents> retrieveIdrepoDocument(String uin)
			throws ApisResourceAccessException {
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
				"Utilities::retrieveIdrepoDocument()::entry");
		ResponseDTO idResponseDto = retrieveIdrepoResponseObj(uin, "type", "all");
		return idResponseDto != null ? idResponseDto.getDocuments() : null;
	}

	/**
	 * Check if uin is present in idrepo
	 *
	 * @param uin
	 * @return
	 * @throws ApisResourceAccessException
	 * @throws IOException
	 */
	public boolean uinPresentInIdRepo(String uin) throws ApisResourceAccessException, IOException {
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
				"Utilities::uinPresentInIdRepo()::entry");
		return idRepoService.findUinFromIdrepo(uin, getGetRegProcessorDemographicIdentity()) != null;
	}

	/**
	 * Check if uin is missing from Id
	 *
	 * @param errorCode
	 * @param id
	 * @param idType
	 * @return
	 */
	public boolean isUinMissingFromIdAuth(String errorCode, String id, String idType) {
		if (errorCode.equalsIgnoreCase("IDA-MLC-018") && idType != null && idType.equalsIgnoreCase("UIN")) {
			try {
				return uinPresentInIdRepo(id);
			} catch (IOException | ApisResourceAccessException exception) {
				regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
						ExceptionUtils.getStackTrace(exception));
				// in case of exception return true so that the request is marked for reprocess
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns all the list of queue details(inbound/outbound address,name,url,pwd)
	 * from abisJson Also validates the abis json fileds(null or not).
	 *
	 * @return the abis queue details
	 * @throws RegistrationProcessorCheckedException the registration processor
	 *                                               checked exception
	 */
	public List<AbisQueueDetails> getAbisQueueDetails() throws RegistrationProcessorCheckedException {
		List<AbisQueueDetails> abisQueueDetailsList = new ArrayList<>();

		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"Utilities::getAbisQueueDetails()::entry");

		try {
			JSONObject abisJson = jsonCache.computeIfAbsent("abisJson", k -> {
				try {
					String jsonString = getJson(configServerFileStorageURL, registrationProcessorAbisJson);
					return JsonUtil.objectMapperReadValue(jsonString, JSONObject.class);
				} catch (IOException e) {
					throw new RegistrationProcessorUnCheckedException(
							PlatformErrorMessages.RPR_SYS_IO_EXCEPTION.getCode(),
							PlatformErrorMessages.RPR_SYS_IO_EXCEPTION.getMessage(), e);
				}
			});

			JSONArray regProcessorAbisArray = JsonUtil.getJSONArray(abisJson, ABIS);
			regProcessorAbisArray.parallelStream().forEach(jsonObject -> {
				AbisQueueDetails abisQueueDetails = new AbisQueueDetails();
				JSONObject json = new JSONObject((Map) jsonObject);
				String userName = validateAbisQueueJsonAndReturnValue(json, USERNAME);
				String password = validateAbisQueueJsonAndReturnValue(json, PASSWORD);
				String brokerUrl = validateAbisQueueJsonAndReturnValue(json, BROKERURL);
				String failOverBrokerUrl = FAIL_OVER + brokerUrl + "," + brokerUrl + RANDOMIZE_FALSE;
				String typeOfQueue = validateAbisQueueJsonAndReturnValue(json, TYPEOFQUEUE);
				String inboundQueueName = validateAbisQueueJsonAndReturnValue(json, INBOUNDQUEUENAME);
				String outboundQueueName = validateAbisQueueJsonAndReturnValue(json, OUTBOUNDQUEUENAME);
				String queueName = validateAbisQueueJsonAndReturnValue(json, NAME);
				int inboundMessageTTL = validateAbisQueueJsonAndReturnIntValue(json, INBOUNDMESSAGETTL);

				MosipQueue mosipQueue = mosipConnectionFactory.createConnection(typeOfQueue, userName, password,
						failOverBrokerUrl, trustedPackages);
				if (mosipQueue == null) {
					throw new QueueConnectionNotFound(
							PlatformErrorMessages.RPR_PIS_ABIS_QUEUE_CONNECTION_NULL.getMessage());
				}

				abisQueueDetails.setMosipQueue(mosipQueue);
				abisQueueDetails.setInboundQueueName(inboundQueueName);
				abisQueueDetails.setOutboundQueueName(outboundQueueName);
				abisQueueDetails.setName(queueName);
				abisQueueDetails.setInboundMessageTTL(inboundMessageTTL);
				abisQueueDetailsList.add(abisQueueDetails);
			});
		} catch (Exception e) {
			throw new RegistrationProcessorCheckedException(PlatformErrorMessages.RPR_SYS_IO_EXCEPTION.getCode(),
					PlatformErrorMessages.RPR_SYS_IO_EXCEPTION.getMessage(), e);
		}
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"Utilities::getAbisQueueDetails()::exit");

		return abisQueueDetailsList;
	}

	/**
	 * Gets registration processor mapping json from config and maps to
	 * RegistrationProcessorIdentity java class.
	 *
	 * @return the registration processor identity json
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public JSONObject getRegistrationProcessorMappingJson(String packetSegment) throws IOException {
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"Utilities::getRegistrationProcessorMappingJson()::entry");

		String cacheKey = "mappingJson_" + packetSegment;
		JSONObject mappingJson = jsonCache.computeIfAbsent(cacheKey, k -> {
			try {
				String mappingJsonString = getJson(configServerFileStorageURL, getRegProcessorIdentityJson);
				return objMapper.readValue(mappingJsonString, JSONObject.class);
			} catch (IOException e) {
				throw new RegistrationProcessorUnCheckedException(PlatformErrorMessages.RPR_SYS_IO_EXCEPTION.getCode(),
						PlatformErrorMessages.RPR_SYS_IO_EXCEPTION.getMessage(), e);
			}
		});

		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"Utilities::getRegistrationProcessorMappingJson()::exit");

		return JsonUtil.getJSONObject(mappingJson, packetSegment);
	}

	public String getMappingJsonValue(String key, String packetSegment) throws IOException {
		JSONObject jsonObject = getRegistrationProcessorMappingJson(packetSegment);
		Object obj = jsonObject.get(key);
		return obj instanceof Map ? (String) ((Map) obj).get("value") : (obj != null ? obj.toString() : null);

	}

	/**
	 * Gets the elapse status.
	 *
	 * @param registrationStatusDto the registration status dto
	 * @param transactionType       the transaction type
	 * @return the elapse status
	 */
	public String getElapseStatus(InternalRegistrationStatusDto registrationStatusDto, String transactionType) {
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"Utilities::getElapseStatus()::entry");

		if (registrationStatusDto.getLatestTransactionTypeCode().equalsIgnoreCase(transactionType)) {
			LocalDateTime createdDateTime = registrationStatusDto.getCreateDateTime();
			LocalDateTime currentDateTime = LocalDateTime.now();
			Duration duration = Duration.between(createdDateTime, currentDateTime);
			long secondsDiffernce = duration.getSeconds();
			if (secondsDiffernce > elapseTime) {
				regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
						"Utilities::getElapseStatus()::exit and value is:  " + RE_PROCESSING);

				return RE_PROCESSING;
			} else {
				regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
						"Utilities::getElapseStatus()::exit and value is:  " + HANDLER);

				return HANDLER;
			}
		}
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"Utilities::getElapseStatus()::exit and value is:  " + NEW_PACKET);

		return NEW_PACKET;
	}

	/**
	 * Gets the latest transaction id.
	 *
	 * @param registrationId the registration id
	 * @return the latest transaction id
	 */
	public String getLatestTransactionId(String registrationId, String process, int iteration,
			String workflowInstanceId) {
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
				registrationId, "Utilities::getLatestTransactionId()::entry");

		String cacheKey = registrationId + "_" + process + "_" + iteration + "_" + workflowInstanceId;

		String latestTransactionId = transactionIdCache.computeIfAbsent(cacheKey, k -> {
			RegistrationStatusEntity entity = registrationStatusDao.find(registrationId, process, iteration,
					workflowInstanceId);

			return entity != null ? entity.getLatestRegistrationTransactionId() : null;
		});

		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
				registrationId, "Utilities::getLatestTransactionId()::exit");

		return latestTransactionId;
	}

	/**
	 * retrieve UIN from IDRepo by registration id.
	 *
	 * @param regId the reg id
	 * @return the JSON object
	 * @throws ApisResourceAccessException the apis resource access exception
	 * @throws IdRepoAppException          the id repo app exception
	 * @throws IOException                 Signals that an I/O exception has
	 *                                     occurred.
	 */
	public JSONObject idrepoRetrieveIdentityByRid(String regId)
			throws ApisResourceAccessException, IdRepoAppException, IOException {
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
				regId, "Utilities::idrepoRetrieveIdentityByRid()::entry");

		if (regId != null) {
			List<String> pathSegments = new ArrayList<>();
			pathSegments.add(regId);
			regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					regId,
					"Utilities::idrepoRetrieveIdentityByRid():: RETRIEVEIDENTITYFROMRID GET service call Started");

			IdResponseDTO1 idResponseDto = (IdResponseDTO1) restClientService.getApi(ApiName.RETRIEVEIDENTITYFROMRID,
					pathSegments, "", "", IdResponseDTO1.class);

			regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
					"Utilities::idrepoRetrieveIdentityByRid():: RETRIEVEIDENTITYFROMRID GET service call ended successfully");

			if (idResponseDto == null || !idResponseDto.getErrors().isEmpty()) {
				regProcLogger.error(LoggerFileConstant.SESSIONID.toString(),
						LoggerFileConstant.REGISTRATIONID.toString(), regId,
						"Utilities::idrepoRetrieveIdentityByRid():: error with error message "
								+ PlatformErrorMessages.RPR_PVM_INVALID_UIN.getMessage() + " "
								+ idResponseDto.getErrors().toString());
				throw new IdRepoAppException(
						PlatformErrorMessages.RPR_PVM_INVALID_UIN.getMessage() + idResponseDto.getErrors().toString());
			}
			try {
				String response = objMapper.writeValueAsString(idResponseDto.getResponse().getIdentity());
				return (JSONObject) new JSONParser().parse(response);
			} catch (org.json.simple.parser.ParseException e) {
				regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
						ExceptionUtils.getStackTrace(e));
				throw new IdRepoAppException("Error while parsing string to JSONObject", e);
			}

		}
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(), "",
				"Utilities::retrieveUIN()::exit regId is null");

		return null;
	}

	/**
	 * Validate abis queue json and return value.
	 *
	 * @param jsonObject the json object
	 * @param key        the key
	 * @return the string
	 */
	private String validateAbisQueueJsonAndReturnValue(JSONObject jsonObject, String key) {
		String value = JsonUtil.getJSONValue(jsonObject, key);
		if (value == null) {
			throw new RegistrationProcessorUnCheckedException(
					PlatformErrorMessages.ABIS_QUEUE_JSON_VALIDATION_FAILED.getCode(),
					PlatformErrorMessages.ABIS_QUEUE_JSON_VALIDATION_FAILED.getMessage() + "::" + key);
		}

		return value;
	}

	/**
	 * Validate abis queue json and return long value.
	 *
	 * @param jsonObject the json object
	 * @param key        the key
	 * @return the long value
	 */
	private int validateAbisQueueJsonAndReturnIntValue(JSONObject jsonObject, String key) {
		Integer value = JsonUtil.getJSONValue(jsonObject, key);
		if (value == null) {
			throw new RegistrationProcessorUnCheckedException(
					PlatformErrorMessages.ABIS_QUEUE_JSON_VALIDATION_FAILED.getCode(),
					PlatformErrorMessages.ABIS_QUEUE_JSON_VALIDATION_FAILED.getMessage() + "::" + key);
		}

		return value.intValue();
	}

	/**
	 * Gets the uin by vid.
	 *
	 * @param vid the vid
	 * @return the uin by vid
	 * @throws ApisResourceAccessException the apis resource access exception
	 * @throws VidCreationException        the vid creation exception
	 */
	@SuppressWarnings("unchecked")
	public String getUinByVid(String vid) throws ApisResourceAccessException, VidCreationException {
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(), "",
				"Utilities::getUinByVid():: entry");

		List<String> pathSegments = new ArrayList<>();
		pathSegments.add(vid);

		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(), "",
				"Stage::methodname():: RETRIEVEIUINBYVID GET service call Started");

		VidResponseDTO response = (VidResponseDTO) restClientService.getApi(ApiName.GETUINBYVID, pathSegments, "", "",
				VidResponseDTO.class);
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
				"Utilities::getUinByVid():: RETRIEVEIUINBYVID GET service call ended successfully");

		if (response == null || response.getResponse() == null || !response.getErrors().isEmpty()) {
			throw new VidCreationException(PlatformErrorMessages.RPR_PGS_VID_EXCEPTION.getMessage(),
					"VID creation exception");
		}
		return response.getResponse().getUin();
	}

	/**
	 * Retrieve idrepo json status.
	 *
	 * @param uin the uin
	 * @return the string
	 * @throws ApisResourceAccessException the apis resource access exception
	 * @throws IdRepoAppException          the id repo app exception
	 */
	public String retrieveIdrepoJsonStatus(String uin) throws ApisResourceAccessException, IdRepoAppException {
		String response = null;
		if (uin != null) {
			regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
					"Utilities::retrieveIdrepoJson()::entry");
			List<String> pathSegments = new ArrayList<>();
			pathSegments.add(uin);

			IdResponseDTO1 idResponseDto = (IdResponseDTO1) restClientService.getApi(ApiName.IDREPOGETIDBYUIN,
					pathSegments, "", "", IdResponseDTO1.class);
			if (idResponseDto == null || idResponseDto.getResponse() == null) {
				regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
						"Utilities::retrieveIdrepoJson()::exit idResponseDto is null");
				return null;
			}
			if (!idResponseDto.getErrors().isEmpty()) {
				List<ErrorDTO> error = idResponseDto.getErrors();
				regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
						"Utilities::retrieveIdrepoJson():: error with error message " + error.get(0).getMessage());
				throw new IdRepoAppException(error.get(0).getMessage());
			}

			response = idResponseDto.getResponse().getStatus();

			regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
					"Utilities::retrieveIdrepoJson():: IDREPOGETIDBYUIN GET service call ended Successfully");
		}

		return response;
	}

	public String getRefId(String id, String refId) {
		if (StringUtils.isNotEmpty(refId))
			return refId;

		String centerId = id.substring(0, centerIdLength);
		String machineId = id.substring(centerIdLength, centerIdLength + machineIdLength);
		return centerId + "_" + machineId;
	}
}