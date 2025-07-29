package io.mosip.registration.processor.core.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.registration.processor.core.code.ApiName;
import io.mosip.registration.processor.core.constant.LoggerFileConstant;
import io.mosip.registration.processor.core.exception.ApisResourceAccessException;
import io.mosip.registration.processor.core.http.ResponseWrapper;
import io.mosip.registration.processor.core.kernel.master.dto.LanguageDto;
import io.mosip.registration.processor.core.kernel.master.dto.LanguageResponseDto;
import io.mosip.registration.processor.core.logger.RegProcessorLogger;
import io.mosip.registration.processor.core.spi.restclient.RegistrationProcessorRestClientService;
import io.mosip.registration.processor.core.util.exception.LanguagesUtilException;

@Component
public class LanguageUtility {
	/** The reg proc logger. */
	private static final Logger regProcLogger = RegProcessorLogger.getLogger(LanguageUtility.class);

	@Autowired
	private RegistrationProcessorRestClientService<Object> registrationProcessorRestService;
	
	@Autowired
	private ObjectMapper mapper;
	
	/** Cache for storing LanguageResponseDto to avoid repeated REST calls. */
    private final ConcurrentHashMap<String, LanguageResponseDto> languageCache = new ConcurrentHashMap<>();
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public String getLangCodeFromNativeName(String nativeName) {
        regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
                "LanguageUtility::getLangCodeFromNativeName()::entry");

        String langCode = null;
        try {
            // Use a fixed cache key, since the API returns all languages regardless of input
            String cacheKey = "LANGUAGES";
            LanguageResponseDto languageResponseDto = languageCache.get(cacheKey);

            // If not cached, call the API
            if (languageResponseDto == null) {
                ResponseWrapper<LanguageResponseDto> response =
                        (ResponseWrapper) registrationProcessorRestService.getApi(ApiName.LANGUAGE, null, "", "", ResponseWrapper.class);

                if (response == null || response.getResponse() == null) {
                    throw new LanguagesUtilException("No language data received from master data service.");
                }

                if (response.getErrors() != null && !response.getErrors().isEmpty()) {
                    response.getErrors().forEach(error -> regProcLogger.error(
                            LoggerFileConstant.SESSIONID.toString(),
                            LoggerFileConstant.UIN.toString(),
                            "",
                            "LanguageUtility::getLangCodeFromNativeName():: Error - " + error.getMessage()
                    ));
                }

                languageResponseDto = mapper.convertValue(response.getResponse(), LanguageResponseDto.class);
                languageCache.put(cacheKey, languageResponseDto); // Cache the response
            }

            if (languageResponseDto.getLanguages() != null) {
                for (LanguageDto dto : languageResponseDto.getLanguages()) {
                    if (dto != null && (
                            nativeName.equalsIgnoreCase(dto.getNativeName()) ||
                            nativeName.equalsIgnoreCase(dto.getName()) ||
                            nativeName.equalsIgnoreCase(dto.getCode()))) {
                        langCode = dto.getCode();
                        break;
                    }
                }
            }

        } catch (ApisResourceAccessException | IllegalArgumentException e) {
            regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.UIN.toString(), "",
                    "LanguageUtility::getLangCodeFromNativeName():: Exception - " + e.getMessage());
            throw new LanguagesUtilException(e.getMessage(), e);
        }

        regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
                "LanguageUtility::getLangCodeFromNativeName()::exit");

        return langCode;
    }
}