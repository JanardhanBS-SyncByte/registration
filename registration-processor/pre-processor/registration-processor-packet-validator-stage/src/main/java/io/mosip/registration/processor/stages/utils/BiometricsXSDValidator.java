package io.mosip.registration.processor.stages.utils;

import io.mosip.kernel.biometrics.commons.CbeffValidator;
import io.mosip.kernel.biometrics.entities.BIR;
import io.mosip.kernel.biometrics.entities.BiometricRecord;
import io.mosip.kernel.cbeffutil.container.impl.CbeffContainerImpl;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

@Component
public class BiometricsXSDValidator {

    @Value("${mosip.kernel.xsdstorage-uri}")
    private String configServerFileStorageURL;
    
    @Value("${mosip.kernel.xsdfile}")
    private String schemaFileName;
    
    private byte[] xsd = null;

    private final ReentrantLock lock = new ReentrantLock();
    
    /**
     * Validates the given BiometricRecord against the configured XSD schema.
     *
     * @param biometricRecord the biometric record to validate
     * @throws IOException if schema cannot be loaded
     * @throws IllegalArgumentException if biometric data fails schema validation
     */
    public void validateXSD(BiometricRecord biometricRecord ) throws Exception  {
    	ensureSchemaLoaded();

        CbeffContainerImpl cbeffContainer = new CbeffContainerImpl();
        BIR bir = cbeffContainer.createBIRType(biometricRecord.getSegments());

        try {
            CbeffValidator.createXMLBytes(bir, xsd); // Throws if invalid
        } catch (Exception e) {
            throw new IllegalArgumentException("Biometric record failed XSD validation: " + e.getMessage(), e);
        }
    } 

    /**
     * Ensures that the XSD schema is loaded and cached in memory.
     */
    private void ensureSchemaLoaded() throws IOException {
        if (xsd == null) {
            lock.lock();
            try {
                if (xsd == null) {
                    String fullUrl = configServerFileStorageURL.endsWith("/") ?
                            configServerFileStorageURL + schemaFileName :
                            configServerFileStorageURL + "/" + schemaFileName;

                    try (InputStream inputStream = new URL(fullUrl).openStream()) {
                        xsd = IOUtils.toByteArray(inputStream);
                    } catch (MalformedURLException e) {
                        throw new IOException("Invalid schema URL: " + fullUrl, e);
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }
}