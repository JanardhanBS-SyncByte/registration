package io.mosip.registration.processor.packet.manager.idreposervice.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.registration.processor.core.code.ApiName;
import io.mosip.registration.processor.core.constant.AbisConstant;
import io.mosip.registration.processor.core.constant.IdType;
import io.mosip.registration.processor.core.constant.LoggerFileConstant;
import io.mosip.registration.processor.core.exception.ApisResourceAccessException;
import io.mosip.registration.processor.core.http.ResponseWrapper;
import io.mosip.registration.processor.core.idrepo.dto.IdResponseDTO;
import io.mosip.registration.processor.core.idrepo.dto.ResponseDTO;
import io.mosip.registration.processor.core.logger.RegProcessorLogger;
import io.mosip.registration.processor.core.spi.restclient.RegistrationProcessorRestClientService;
import io.mosip.registration.processor.core.util.JsonUtil;
import io.mosip.registration.processor.packet.manager.idreposervice.IdRepoService;

/**
 * Service implementation for interacting with the identity repository to retrieve UIN or demographic data.
 * Optimized for performance with efficient JSON processing and robust error handling.
 *
 * @author Nagalakshmi
 * @author Horteppa
 */
@RefreshScope
@Service
public class IdRepoServiceImpl implements IdRepoService {

	/** The logger instance for logging operations and errors. */
	private static final Logger logger = RegProcessorLogger.getLogger(IdRepoServiceImpl.class);

	/** The REST client service for making API calls. */
	@Autowired
	private RegistrationProcessorRestClientService<Object> restClientService;
	
	/** The ObjectMapper for JSON serialization/deserialization. */
	@Autowired
	private ObjectMapper mapper;

	/**
     * Retrieves the UIN (Unique Identification Number) for a given RID (Registration ID).
     *
     * @param rid                           the registration ID (must not be null)
     * @param regProcessorDemographicIdentity the key for the demographic identity field (must not be null)
     * @return the UIN string
     * @throws ApisResourceAccessException if the API call fails
     * @throws IdRepoAccessException if the response is invalid or UIN is not found
     * @throws IOException if JSON processing fails
     */
	@SuppressWarnings("unchecked")
	@Override
	public String getUinByRid(String rid, String regProcessorDemographicIdentity)
            throws IOException, ApisResourceAccessException {
        logger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(), rid,
                "IdRepoServiceImpl::getUinByRid()::entry");

        List<String> pathSegments = List.of(rid);
        String uin = getUin(pathSegments, regProcessorDemographicIdentity, IdType.RID);

        logger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(), rid,
                "IdRepoServiceImpl::getUinByRid()::exit");
        return uin;
    }

	/**
     * Internal method to retrieve UIN for a given ID type and path segments.
     *
     * @param pathSegments                  the path segments for the API call
     * @param regProcessorDemographicIdentity the key for the demographic identity field
     * @param idType                        the ID type (RID, UIN, VID)
     * @return the UIN string
     * @throws ApisResourceAccessException if the API call fails
     * @throws IdRepoAccessException if the response is invalid or UIN is not found
     * @throws IOException if JSON processing fails
     */
	@SuppressWarnings("unchecked")
	private String getUin(List<String> pathSegments, String identityKey, IdType idType)
            throws IOException, ApisResourceAccessException {

        ApiName apiName = switch (idType) {
            case RID -> ApiName.RETRIEVEIDENTITYFROMRID;
            case UIN -> ApiName.IDREPOGETIDBYUIN;
            case VID -> ApiName.GETUINBYVID;
            default -> ApiName.RETRIEVEIDENTITY;
        };

        ResponseWrapper<IdResponseDTO> response = (ResponseWrapper<IdResponseDTO>) restClientService.getApi(
                apiName, pathSegments, "", "", ResponseWrapper.class);

        if (response.getResponse() != null) {
            String jsonString = mapper.writeValueAsString(response.getResponse());
            JSONObject identityJson = JsonUtil.objectMapperReadValue(jsonString, JSONObject.class);
            if (identityJson != null) {
                JSONObject demographicIdentity = JsonUtil.getJSONObject(identityJson, identityKey);
                if (demographicIdentity != null) {
                    return JsonUtil.getJSONValue(demographicIdentity, AbisConstant.UIN);
                }
            }
        }
        return null;
    }

	/**
     * Retrieves the UIN for a given UIN.
     *
     * @param uin                           the UIN (must not be null)
     * @param regProcessorDemographicIdentity the key for the demographic identity field (must not be null)
     * @return the UIN string
     * @throws ApisResourceAccessException if the API call fails
     * @throws IdRepoAccessException if the response is invalid or UIN is not found
     * @throws IOException if JSON processing fails
     */
	@Override
	public String findUinFromIdrepo(String uin, String regProcessorDemographicIdentity)
            throws IOException, ApisResourceAccessException {
        logger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), uin,
                "IdRepoServiceImpl::findUinFromIdrepo()::entry");

        List<String> pathSegments = List.of(uin);
        String result = getUin(pathSegments, regProcessorDemographicIdentity, IdType.UIN);

        logger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), uin,
                "IdRepoServiceImpl::findUinFromIdrepo()::exit");
        return result;
    }

	/**
     * Retrieves the demographic JSON data for a given RID.
     *
     * @param machedRegId                   the registration ID (must not be null)
     * @param regProcessorDemographicIdentity the key for the demographic identity field (must not be null)
     * @return the demographic JSON object
     * @throws ApisResourceAccessException if the API call fails
     * @throws IdRepoAccessException if the response is invalid or demographic data is not found
     * @throws IOException if JSON processing fails
     */
	@SuppressWarnings("unchecked")
	@Override
	public JSONObject getIdJsonFromIDRepo(String machedRegId, String regProcessorDemographicIdentity)
			throws IOException, ApisResourceAccessException {
		logger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
				machedRegId, "IdRepoServiceImpl::getIdJsonFromIDRepo()::entry");

		List<String> pathSegments = List.of(machedRegId);
        ResponseWrapper<IdResponseDTO> response = (ResponseWrapper<IdResponseDTO>) restClientService.getApi(
                ApiName.RETRIEVEIDENTITYFROMRID, pathSegments, "", "", ResponseWrapper.class);

        if (response.getResponse() != null) {
            String jsonString = mapper.writeValueAsString(response.getResponse());
            JSONObject identityJson = JsonUtil.objectMapperReadValue(jsonString, JSONObject.class);
            if (identityJson != null) {
                JSONObject demographicJson = JsonUtil.getJSONObject(identityJson, regProcessorDemographicIdentity);
                logger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(), machedRegId,
                        "IdRepoServiceImpl::getIdJsonFromIDRepo()::exit");
                return demographicJson;
            }
        }
		logger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
				machedRegId, "IdRepoServiceImpl::getIdJsonFromIDRepo()::exit");

		return null;
	}

	/**
     * Retrieves the ResponseDTO for a given RID with query parameter type=ALL.
     *
     * @param machedRegId the registration ID (must not be null)
     * @return the ResponseDTO object
     * @throws ApisResourceAccessException if the API call fails
     * @throws IdRepoAccessException if the response is invalid
     * @throws IOException if JSON processing fails
     */
	@SuppressWarnings("unchecked")
	@Override
	public ResponseDTO getIdResponseFromIDRepo(String machedRegId) throws IOException, ApisResourceAccessException {
		logger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
				machedRegId, "IdRepoServiceImpl::getIdResponseFromIDRepo()::entry");
		
		List<String> pathSegments = List.of(machedRegId);
        ResponseWrapper<ResponseDTO> response = (ResponseWrapper<ResponseDTO>) restClientService.getApi(
                ApiName.IDREPOGETIDBYUIN, pathSegments, "type", "ALL", ResponseWrapper.class);

        if (response.getResponse() != null) {
            ResponseDTO dto = mapper.readValue(mapper.writeValueAsString(response.getResponse()), ResponseDTO.class);
            logger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(), machedRegId	,
                    "IdRepoServiceImpl::getIdResponseFromIDRepo()::exit");
            return dto;
        }
        
		logger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
				machedRegId, "IdRepoServiceImpl::getIdResponseFromIDRepo()::exit");

		return null;
	}
}