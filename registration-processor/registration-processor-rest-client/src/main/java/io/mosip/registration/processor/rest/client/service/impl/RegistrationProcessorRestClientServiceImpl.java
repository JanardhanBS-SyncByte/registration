package io.mosip.registration.processor.rest.client.service.impl;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import io.mosip.kernel.core.exception.ExceptionUtils;
import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.registration.processor.core.code.ApiName;
import io.mosip.registration.processor.core.constant.LoggerFileConstant;
import io.mosip.registration.processor.core.exception.ApisResourceAccessException;
import io.mosip.registration.processor.core.exception.util.PlatformErrorMessages;
import io.mosip.registration.processor.core.logger.RegProcessorLogger;
import io.mosip.registration.processor.core.spi.restclient.RegistrationProcessorRestClientService;
import io.mosip.registration.processor.rest.client.utils.RestApiClient;

/**
 * The Class RegistrationProcessorRestClientServiceImpl.
 * 
 * @author Rishabh Keshari
 */
@Service
public class RegistrationProcessorRestClientServiceImpl implements RegistrationProcessorRestClientService<Object> {

	/** The logger. */
	private static final Logger regProcLogger = RegProcessorLogger
			.getLogger(RegistrationProcessorRestClientServiceImpl.class);

	/** The rest api client. */
	@Autowired
	private RestApiClient restApiClient;

	/** The env. */
	@Autowired
	private Environment env;

	private final ConcurrentHashMap<ApiName, String> apiHostCache = new ConcurrentHashMap<>();

	private String getApiBase(ApiName apiName) {
		return apiHostCache.computeIfAbsent(apiName, k -> env.getProperty(k.name()));
	}

	private UriComponentsBuilder createUriBuilder(String base, List<String> segments, List<String> names,
			List<Object> values) {
		UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(base);
		if (!CollectionUtils.isEmpty(segments)) {
			segments.stream().filter(s -> s != null && !s.isEmpty()).forEach(builder::pathSegment);
		}
		if (!CollectionUtils.isEmpty(names)) {
			for (int i = 0; i < names.size(); i++) {
				builder.queryParam(names.get(i), values.get(i));
			}
		}
		return builder;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mosip.registration.processor.core.spi.restclient.
	 * RegistrationProcessorRestClientService#getApi(io.mosip.registration.
	 * processor .core.code.ApiName,
	 * io.mosip.registration.processor.core.code.RestUriConstant, java.lang.String,
	 * java.lang.String, java.lang.Class)
	 */
	@Override
	public Object getApi(ApiName apiName, List<String> pathsegments, String queryParam, String queryParamValue,
			Class<?> responseType) throws ApisResourceAccessException {
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::getApi()::entry");

		Object obj = null;
		try {
			String apiHostIpPort = getApiBase(apiName);
			UriComponentsBuilder builder = createUriBuilder(apiHostIpPort, pathsegments, List.of(queryParam),
					List.of(queryParamValue));
			UriComponents uriComponents = builder.build(false).encode();
			obj = restApiClient.getApi(uriComponents.toUri(), responseType);
		} catch (Exception e) {
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					"", e.getMessage() + ExceptionUtils.getStackTrace(e));

			throw new ApisResourceAccessException(PlatformErrorMessages.RPR_RCT_UNKNOWN_RESOURCE_EXCEPTION.getCode(),
					e);

		}
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::getApi()::exit");
		return obj;
	}

	@Override
	public Object getApi(ApiName apiName, List<String> pathsegments, List<String> queryParams,
			List<Object> queryParamValues, Class<?> responseType) throws ApisResourceAccessException {
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::getApi()::entry");
		Object obj = null;
		try {
			String apiHostIpPort = getApiBase(apiName);
			UriComponentsBuilder builder = createUriBuilder(apiHostIpPort, pathsegments, queryParams, queryParamValues);
			UriComponents uriComponents = builder.build(false).encode();
			obj = restApiClient.getApi(uriComponents.toUri(), responseType);
		} catch (Exception e) {
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					"", e.getMessage() + ExceptionUtils.getStackTrace(e));

			throw new ApisResourceAccessException(PlatformErrorMessages.RPR_RCT_UNKNOWN_RESOURCE_EXCEPTION.getCode(),
					e);
		}

		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::getApi()::exit");
		return obj;
	}

	public Object postApi(ApiName apiName, String queryParam, String queryParamValue, Object requestedData,
			Class<?> responseType, MediaType mediaType) throws ApisResourceAccessException {
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::postApi()::entry");

		Object obj = null;
		try {
			String apiHostIpPort = getApiBase(apiName);
			UriComponentsBuilder builder = createUriBuilder(apiHostIpPort, null, List.of(queryParam),
					List.of(queryParamValue));
			obj = restApiClient.postApi(builder.toUriString(), mediaType, requestedData, responseType);
		} catch (Exception e) {
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					"", e.getMessage() + ExceptionUtils.getStackTrace(e));

			throw new ApisResourceAccessException(PlatformErrorMessages.RPR_RCT_UNKNOWN_RESOURCE_EXCEPTION.getMessage(),
					e);

		}
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::postApi()::exit");
		return obj;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mosip.registration.processor.core.spi.restclient.
	 * RegistrationProcessorRestClientService#postApi(io.mosip.registration.
	 * processor.core.code.ApiName,
	 * io.mosip.registration.processor.core.code.RestUriConstant, java.lang.String,
	 * java.lang.String, java.lang.Object, java.lang.Class)
	 */
	@Override
	public Object postApi(ApiName apiName, String queryParamName, String queryParamValue, Object requestedData,
			Class<?> responseType) throws ApisResourceAccessException {
		return postApi(apiName, queryParamName, queryParamValue, requestedData, responseType, null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mosip.registration.processor.core.spi.restclient.
	 * RegistrationProcessorRestClientService#postApi(io.mosip.registration.
	 * processor.core.code.ApiName, java.util.List, java.lang.String,
	 * java.lang.String, java.lang.Object, java.lang.Class)
	 */
	@Override
	public Object postApi(ApiName apiName, List<String> pathsegments, String queryParam, String queryParamValue,
			Object requestedData, Class<?> responseType) throws ApisResourceAccessException {

		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::postApi()::entry");
		Object obj = null;
		try {
			String apiHostIpPort = getApiBase(apiName);
			UriComponentsBuilder builder = createUriBuilder(apiHostIpPort, pathsegments, List.of(queryParam),
					List.of(queryParamValue));
			obj = restApiClient.postApi(builder.toUriString(), null, requestedData, responseType);
		} catch (Exception e) {
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					"", e.getMessage() + ExceptionUtils.getStackTrace(e));

			throw new ApisResourceAccessException(PlatformErrorMessages.RPR_RCT_UNKNOWN_RESOURCE_EXCEPTION.getMessage(),
					e);

		}
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::postApi()::exit");
		return obj;
	}

	@Override
	public Object postApi(ApiName apiName, MediaType mediaType, List<String> pathsegments, List<String> queryParams,
			List<Object> queryParamValues, Object requestedData, Class<?> responseType)
			throws ApisResourceAccessException {

		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::postApi()::entry");
		Object obj = null;

		try {
			String apiHostIpPort = getApiBase(apiName);
			UriComponentsBuilder builder = createUriBuilder(apiHostIpPort, pathsegments, queryParams, queryParamValues);
			obj = restApiClient.postApi(builder.toUriString(), mediaType, requestedData, responseType);

		} catch (Exception e) {
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					"", e.getMessage() + ExceptionUtils.getStackTrace(e));

			throw new ApisResourceAccessException(PlatformErrorMessages.RPR_RCT_UNKNOWN_RESOURCE_EXCEPTION.getMessage(),
					e);

		}
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::postApi()::exit");
		return obj;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see io.mosip.registration.processor.core.spi.restclient.
	 * RegistrationProcessorRestClientService#patchApi(io.mosip.registration.
	 * processor.core.code.ApiName, java.util.List, java.lang.String,
	 * java.lang.String, java.lang.Object, java.lang.Class)
	 */
	public Object patchApi(ApiName apiName, List<String> pathsegments, String queryParam, String queryParamValue,
			Object requestedData, Class<?> responseType) throws ApisResourceAccessException {

		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::postApi()::entry");
		Object obj = null;
		try {
			String apiHostIpPort = getApiBase(apiName);
			UriComponentsBuilder builder = createUriBuilder(apiHostIpPort, pathsegments, List.of(queryParam),
					List.of(queryParamValue));
			obj = restApiClient.patchApi(builder.toUriString(), requestedData, responseType);

		} catch (Exception e) {
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					"", e.getMessage() + ExceptionUtils.getStackTrace(e));

			throw new ApisResourceAccessException(PlatformErrorMessages.RPR_RCT_UNKNOWN_RESOURCE_EXCEPTION.getMessage(),
					e);

		}
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::postApi()::exit");
		return obj;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see io.mosip.registration.processor.core.spi.restclient.
	 * RegistrationProcessorRestClientService#putApi(io.mosip.registration.
	 * processor.core.code.ApiName, java.util.List, java.lang.String,
	 * java.lang.String, java.lang.Object, java.lang.Class)
	 */
	public Object putApi(ApiName apiName, List<String> pathsegments, String queryParam, String queryParamValue,
			Object requestedData, Class<?> responseType, MediaType mediaType) throws ApisResourceAccessException {

		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::putApi()::entry");
		Object obj = null;
		try {
			String apiHostIpPort = getApiBase(apiName);
			UriComponentsBuilder builder = createUriBuilder(apiHostIpPort, pathsegments, List.of(queryParam),
					List.of(queryParamValue));
			obj = restApiClient.putApi(builder.toUriString(), requestedData, responseType, mediaType);

		} catch (Exception e) {
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					"", e.getMessage() + ExceptionUtils.getStackTrace(e));

			throw new ApisResourceAccessException(PlatformErrorMessages.RPR_RCT_UNKNOWN_RESOURCE_EXCEPTION.getMessage(),
					e);
		}
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::putApi()::exit");
		return obj;
	}

	/**
	 * Check null.
	 *
	 * @param queryParam the query param name
	 * @return true, if successful
	 */
	private boolean checkNull(String queryParam) {
		return ((queryParam == null) || (("").equals(queryParam)));
	}

	@Override
	public Object postApi(String url, MediaType mediaType, List<String> pathsegments, List<String> queryParams,
			List<Object> queryParamValues, Object requestedData, Class<?> responseType)
			throws ApisResourceAccessException {
		Object obj = null;
		try {
			if (url != null) {
				UriComponentsBuilder builder = createUriBuilder(url, pathsegments, queryParams, queryParamValues);

				obj = restApiClient.postApi(builder.toUriString(), mediaType, requestedData, responseType);
			}
		} catch (Exception e) {
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					"", e.getMessage() + ExceptionUtils.getStackTrace(e));

			throw new ApisResourceAccessException(PlatformErrorMessages.RPR_RCT_UNKNOWN_RESOURCE_EXCEPTION.getMessage(),
					e);
		}
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::postApi()::exit");
		return obj;
	}

	@Override
	public Integer headApi(ApiName apiName, List<String> pathsegments, List<String> queryParams,
			List<Object> queryParamValues) throws ApisResourceAccessException {
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::headApi()::entry");
		Integer obj = null;
		try {
			String apiHostIpPort = getApiBase(apiName);
			UriComponentsBuilder builder = createUriBuilder(apiHostIpPort, pathsegments, queryParams, queryParamValues);
			UriComponents uriComponents = builder.build(false).encode();
			regProcLogger.debug(uriComponents.toUri().toString(), "URI", "", "");
			obj = restApiClient.headApi(uriComponents.toUri());
		} catch (Exception e) {
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					"", e.getMessage() + ExceptionUtils.getStackTrace(e));

			throw new ApisResourceAccessException(PlatformErrorMessages.RPR_RCT_UNKNOWN_RESOURCE_EXCEPTION.getCode(),
					e);

		}
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::getApi()::exit");
		return obj;
	}

	@Override
	public Object deleteApi(ApiName apiName, List<String> pathsegments, String queryParam, String queryParamValue,
			Class<?> responseType) throws ApisResourceAccessException {
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::deleteApi()::entry");
		Object obj = null;

		try {
			String apiHostIpPort = getApiBase(apiName);
			UriComponentsBuilder builder = createUriBuilder(apiHostIpPort, pathsegments, List.of(queryParam),
					List.of(queryParamValue));
			UriComponents uriComponents = builder.build(false).encode();
			regProcLogger.debug(uriComponents.toUri().toString(), "URI", "", "");
			obj = restApiClient.deleteApi(uriComponents.toUri(), responseType);

		} catch (Exception e) {
			regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
					"", e.getMessage() + ExceptionUtils.getStackTrace(e));

			throw new ApisResourceAccessException(PlatformErrorMessages.RPR_RCT_UNKNOWN_RESOURCE_EXCEPTION.getCode(),
					e);

		}
		regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
				"RegistrationProcessorRestClientServiceImpl::deleteApi::exit");
		return obj;
	}
}