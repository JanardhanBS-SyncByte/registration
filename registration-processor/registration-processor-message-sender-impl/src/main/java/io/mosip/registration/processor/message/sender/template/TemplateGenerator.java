package io.mosip.registration.processor.message.sender.template;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.log.NullLogChute;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.apache.velocity.runtime.resource.loader.FileResourceLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.kernel.core.templatemanager.exception.TemplateMethodInvocationException;
import io.mosip.kernel.core.templatemanager.exception.TemplateParsingException;
import io.mosip.kernel.core.templatemanager.exception.TemplateResourceNotFoundException;
import io.mosip.kernel.core.templatemanager.spi.TemplateManager;
import io.mosip.kernel.templatemanager.velocity.impl.TemplateManagerImpl;
import io.mosip.registration.processor.core.code.ApiName;
import io.mosip.registration.processor.core.constant.LoggerFileConstant;
import io.mosip.registration.processor.core.exception.ApisResourceAccessException;
import io.mosip.registration.processor.core.exception.TemplateProcessingFailureException;
import io.mosip.registration.processor.core.exception.util.PlatformErrorMessages;
import io.mosip.registration.processor.core.http.ResponseWrapper;
import io.mosip.registration.processor.core.logger.RegProcessorLogger;
import io.mosip.registration.processor.core.notification.template.generator.dto.TemplateResponseDto;
import io.mosip.registration.processor.core.spi.restclient.RegistrationProcessorRestClientService;

@Component
public class TemplateGenerator {

    private static final Logger regProcLogger = RegProcessorLogger.getLogger(TemplateGenerator.class);

    /** Cache for storing TemplateResponseDto by langCode and templateTypeCode. */
    private final ConcurrentHashMap<String, TemplateResponseDto> templateCache = new ConcurrentHashMap<>();

    /** Singleton TemplateManager instance. */
    private final TemplateManager templateManager;

    @Autowired
    private RegistrationProcessorRestClientService<Object> restClientService;

    @Autowired
    private ObjectMapper mapper;

    /** Constructor initializes the TemplateManager as a singleton. */
    public TemplateGenerator() {
        this.templateManager = initializeTemplateManager();
    }

    private TemplateManager initializeTemplateManager() {
        Properties props = new Properties();
        props.put(RuntimeConstants.INPUT_ENCODING, StandardCharsets.UTF_8.name());
        props.put(RuntimeConstants.OUTPUT_ENCODING, StandardCharsets.UTF_8.name());
        props.put(RuntimeConstants.ENCODING_DEFAULT, StandardCharsets.UTF_8.name());
        props.put(RuntimeConstants.RESOURCE_LOADER, "classpath");
        props.put(RuntimeConstants.FILE_RESOURCE_LOADER_PATH, ".");
        props.put(RuntimeConstants.FILE_RESOURCE_LOADER_CACHE, true);
        props.put(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS, NullLogChute.class.getName());
        props.put("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
        props.put("file.resource.loader.class", FileResourceLoader.class.getName());

        VelocityEngine engine = new VelocityEngine(props);
        engine.init();
        return new TemplateManagerImpl(engine);
    }

    private String getCacheKey(String langCode, String templateTypeCode) {
        return langCode + "_" + templateTypeCode;
    }

    /**
     * Fetch and merge the template with provided attributes.
     *
     * @param templateTypeCode The template type
     * @param attributes       Template merge variables
     * @param langCode         Language code
     * @return Merged template content as InputStream
     * @throws IOException
     * @throws ApisResourceAccessException
     */
    public InputStream getTemplate(String templateTypeCode, Map<String, Object> attributes, String langCode)
            throws IOException, ApisResourceAccessException {

        String cacheKey = getCacheKey(langCode, templateTypeCode);
        TemplateResponseDto template = templateCache.get(cacheKey);

        try {
            regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
                    "TemplateGenerator::getTemplate()::entry");

            if (template == null) {
                List<String> pathSegments = new ArrayList<>();
                pathSegments.add(langCode);
                pathSegments.add(templateTypeCode);

                ResponseWrapper<?> responseWrapper = (ResponseWrapper<?>) restClientService.getApi(ApiName.TEMPLATES,
                        pathSegments, "", "", ResponseWrapper.class);

                template = mapper.readValue(mapper.writeValueAsString(responseWrapper.getResponse()),
                        TemplateResponseDto.class);
                templateCache.putIfAbsent(cacheKey, template);
            }

            if (template != null && !template.getTemplates().isEmpty()) {
                InputStream stream = new ByteArrayInputStream(
                        template.getTemplates().iterator().next().getFileText().getBytes(StandardCharsets.UTF_8));
                InputStream resultStream = templateManager.merge(stream, attributes);

                regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.USERID.toString(), "",
                        "TemplateGenerator::getTemplate()::exit");
                return resultStream;
            } else {
                throw new TemplateProcessingFailureException(PlatformErrorMessages.RPR_TEM_PROCESSING_FAILURE.getCode());
            }

        } catch (TemplateResourceNotFoundException | TemplateParsingException | TemplateMethodInvocationException e) {
            regProcLogger.error(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(),
                    null, PlatformErrorMessages.RPR_TEM_PROCESSING_FAILURE.name() + e.getMessage()
                            + ExceptionUtils.getStackTrace(e));
            throw new TemplateProcessingFailureException(PlatformErrorMessages.RPR_TEM_PROCESSING_FAILURE.getCode());
        }
    }
}