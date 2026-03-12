package com.ing.datadist.batch.config;

public final class BatchConstants {

    private BatchConstants() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    // Organisation Unit Batch Constants
    public static final String COFACE_OPS_BATCH_FILE_READER_NAME = "organisationUnitReader";
    public static final String COFACE_OPS_BATCH_STEP_NAME = "organisationUnitEnrichmentStep";
    public static final String COFACE_OPS_BATCH_JOB_NAME = "organisationUnitEnrichmentJob";

    // Legal Entity Batch Constants
    public static final String COFACE_LE_BATCH_JOB_NAME = "LegalEntityEnrichmentJob";
    public static final String COFACE_LE_BATCH_PARENT_FILE_READER_NAME = "legalEntityParentReader";
    public static final String COFACE_LE_BATCH_PARENT_STEP_NAME = "legalEntityParentEnrichStep";
    public static final String COFACE_LE_BATCH_CHILD_FILE_READER_NAME = "legalEntityChildReader";
    public static final String COFACE_LE_BATCH_CHILD_STEP_NAME = "legalEntityChildEnrichStep";

    // Incapables Batch Constants
    public static final String INCAPABLES_BATCH_FILE_READER_NAME = "incapablesReader";
    public static final String INCAPABLES_BATCH_STEP_NAME = "incapablesEnrichmentStep";
    public static final String INCAPABLES_BATCH_JOB_NAME = "IncapablesEnrichmentJob";

    // Legal Entity Deletion Batch Constants
    public static final String COFACE_LE_DELETION_BATCH_JOB_NAME = "LegalEntityDeletionJob";
    public static final String COFACE_LE_DELETION_BATCH_FILE_READER_NAME = "legalEntityDeletionReader";
    public static final String COFACE_LE_DELETION_BATCH_STEP_NAME = "legalEntityDeletionEnrichStep";
    public static final String LE_DELETION_API_TASKLET_STEP = "leDeletionApiTaskletStep";

    // Tasklet Step Names
    public static final String ORG_UNIT_SEARCH_API_TASKLET_STEP = "organisationUnitSearchApiTaskletStep";
    public static final String ORG_SEARCH_API_TASKLET_STEP = "organisationSearchApiTaskletStep";

    // Batch Processing Configuration
    public static final int DEFAULT_CHUNK_SIZE = 100;
    public static final int DEFAULT_RETRY_LIMIT = 3;
    public static final int LINES_TO_SKIP = 1;
}

package com.ing.datadist.batch.config;

import com.ing.datadist.batch.processor.*;
import com.ing.datadist.batch.processor.OrganisationUnitEnrichmentProcessor;
import com.ing.datadist.batch.tasklet.LEDeletionApiTasklet;
import com.ing.datadist.batch.tasklet.OrganisationSearchApiTasklet;
import com.ing.datadist.batch.tasklet.OrganisationUnitSearchApiTasklet;
import com.ing.datadist.dao.LegalEntityDeletionDAO;
import com.ing.datadist.dao.MappingDAO;
import com.ing.datadist.dao.queries.IncapablesQueries;
import com.ing.datadist.dao.ErrorLogDAO;
import com.ing.datadist.domain.*;
import com.ing.datadist.domain.fileinput.LegalEntityParent;
import com.ing.datadist.domain.fileinput.OrganisationUnit;
import com.ing.datadist.domain.incapables.IncapableRelationship;
import com.ing.datadist.ecs.services.FileProviderService;
import com.ing.datadist.retention.config.DataRetentionConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;
import com.ing.datadist.enums.ErrorType;
import com.ing.datadist.enums.RecordType;
import com.ing.datadist.service.FileIngestionService;

import javax.sql.DataSource;

import static com.ing.datadist.batch.config.BatchConstants.*;
import static com.ing.datadist.domain.fileinput.Incapables.*;
import static com.ing.datadist.domain.fileinput.LegalEntityChild.*;
import static com.ing.datadist.domain.fileinput.LegalEntityParent.*;
import static com.ing.datadist.domain.fileinput.OrganisationUnit.*;

@Slf4j
@Configuration
public class BatchJobConfig {

    private static final String COFACE_OPS_TABLE_INSERT_SQL =
            "INSERT INTO DD_COFACEOPS_TBL (OPS_UUID, OPS_NUMBER, ORG_UUID, ORG_NUMBER, OPS_ORGANISATIONUNITNAME, " +
                    "ADDRESS, ADR_POSTALADDRESS_STREETNM, ADR_POSTALADDRESS_HOUSENUM, ADR_POSTALADDRESS_HOUSEADD, " +
                    "ADR_POSTALADDRESS_POSTALCD, ADR_POSTALADDRESS_CITYNAME, ADR_POSTALADDRESS_CNTRYCD, " +
                    "OPS_EXTID_DATEOFISSUE, OPS_EXTID_EXPIRYDATE, " +
                    "ADR_DIGITALADDR_FULLTEL, ADR_DIGITALADDR_EMAIL, " +
                    "FILE_ID) " +
                    "VALUES (:opsUUID, :opsExternalIdentifierVal, :orgUUID, :orgExternalIdentifierVal, :organisationUnitName, " +
                    ":address, :postalAddressStreetNm, :postalAddressHouseNum, :postalAddressHouseAdd, " +
                    ":postalAddressPostalCd, :postalAddressCityName, :postalAddressCntryCd, " +
                    ":opsExtIdDateOfIssue, :opsExtIdExpiryDate, " +
                    ":digitalAddrFullTel, :digitalAddrEmail, " +
                    ":fileId)";
    private final static String IP_EXTERNAL_VAL = "OPS_ExternalIdentifier_Val";
    private final static String LE_EXTERNAL_VAL = "LE_ExternalIdentifier_Val";
    private final static String OU_ORGANISATIOIN_UNITNAME = "OPS_OrganisationUnitName";
    private final static String ADDRESS = "ADDRESS";
    private final static String ADR_POSTALADDRESS_STREETNM = "Adr_PostalAddress_StreetNm";
    private final static String ADR_POSTALADDRESS_HOUSENUM = "Adr_PostalAddress_HouseNum";
    private final static String ADR_POSTALADDRESS_HOUSEDD = "Adr_PostalAddress_HouseAdd";
    private final static String ADR_POSTALADDRESS_POSTALCD = "Adr_PostalAddress_PostalCd";
    private final static String ADR_POSTALADDRESS_CITYNAME = "Adr_PostalAddress_CityName";
    private final static String ADR_POSTALADDRESS_CNTRYCD = "Adr_PostalAddress_CntryCd";
    private final static String IP_EXTID_DATEOFISSUE = "IP_ExtId_DateOfIssue";
    private final static String IP_EXTId_EXPIRYDATE = "IP_ExtId_ExpiryDate";
    private final static String ADR_DIGITALADDR_FULLTEL = "ADR_Digitaladdr_Fulltel";
    private final static String ADR_DIGITALADDR_EMAIL = "Adr_DigitalAddr_Email";
    private final static String COFACE_OPS_BATCH_FILE_READER_NAME = "organisationUnitReader";
    private final static String COFACE_OPS_BATCH_STEP_NAME = "organisationUnitEnrichmentStep";
    private final static String COFACE_OPS_BATCH_JOB_NAME = "organisationUnitEnrichmentJob";
    private final static String COFACE_LE_BATCH_JOB_NAME = "LegalEntityEnrichmentJob";
    private final static String COFACE_LE_BATCH_FILE_READER_NAME = "organisationReader";
    private final static String COFACE_LE_BATCH_PARENT_FILE_READER_NAME = "legalEntityParentReader";
    private final static String COFACE_LE_BATCH_CHILD_FILE_READER_NAME = "legalEntityChildReader";
    String LEGAL_ENTITY_PARENT_TABLE_INSERT_SQL =
            "INSERT INTO DD_LEGAL_ENTITY_PARENT_TBL (" +
                    "DD_UUID, ORG_STATUS, ORG_NAME, ORG_OTHER_NAME, ORG_OTHER_NAME_TYPE, " +
                    "ADR_UNSTRUCTURED_ADDRESS, ADR_POSTALADDRESS_STREETNM, ADR_POSTALADDRESS_HOUSENUM, " +
                    "ADR_POSTALADDRESS_HOUSEADD, ADR_POSTALADDRESS_POSTALCD, ADR_POSTALADDRESS_CITYNAME, " +
                    "ADR_POSTALADDRESS_CNTRYCD, ORG_LEGAL_FORM, ORG_BUSINESS_CLOSE_DOWN_DATE, " +
                    "LE_LEGAL_STATUS, ORG_DATE_OF_FOUNDATION, LE_PREFERRED_LANGUAGE, " +
                    "ADR_DIGITALADDR_FULLTEL, NSSO_EXTL_IDENTIFIER, " +
                    "ASMT_VAT_STATUS, NSSO_EXTL_IDENTIFIER_STATUS, " +
                    "FILE_ID) " +
                    "VALUES (" +
                    ":orgUUID, :orgStatus, :orgName, :orgOtherName, :orgOtherNameType, " +
                    ":adrUnstructuredAddress, :adrPostalAddressStreetName, :adrPostalAddressHouseNum, " +
                    ":adrPostalAddressHouseNumAdd, :adrPostalAddressPostalCode, :adrPostalAddressCityName, " +
                    ":adrPostalAddressCountryCode, :orgLegalForm, :orgBusinessClosedownDate, " +
                    ":lELegalStatus, :asmtIdVerifyApprovalDate, :lEPreferredLanguage, " +
                    ":adrDigitalAddrFullTel, :nssoExternalIdentifier, " +
                    ":asmtVatStatus, :nssoExternalIdentifierStatus, " +
                    ":fileId)";
    String LEGAL_ENTITY_CHILD_TABLE_INSERT_SQL =
            "INSERT INTO DD_LEGAL_ENTITY_CHILD_TBL (" +
                    "DD_UUID, INDUSTRY_CLASS_CODE, INDUSTRY_CLASS_CODE_RANK, INDUSTRY_CLASS_EFF_DATE, INDUSTRY_CLASS_END_DATE, " +
                    "FILE_ID) " +
                    "VALUES (" +
                    ":orgUUID, :industryClassCode, :industryClassRank, :industryClassEffDate, :industryClassEndDate, :fileId)";

    @Autowired(required = false)
    FileProviderService fileProviderService;
    @Autowired
    private MappingDAO mappingDAO;
    @Autowired
    private ErrorLogDAO errorLogDAO;
    @Autowired
    private TaskletCofaceOpsDomainWrapper taskletCofaceOpsDomainWrapper;
    @Autowired
    private TaskletCofaceParentDomainWrapper taskletCofaceParentDomainWrapper;
    @Autowired
    private TaskletCofaceChildDomainWrapper taskletCofaceChildDomainWrapper;
    @Autowired
    private TaskletLeDeletionDomainWrapper taskletLeDeletionDomainWrapper;
    @Autowired
    private LegalEntityDeletionDAO legalEntityDeletionDAO;
    @Autowired
    private FileIngestionService fileIngestionService;
    @Value("${ecs.bucketName}")
    private String bucketName;
    @Value("${ecs.cofaceOUFileName}")
    private String cofaceOUFileName;
    @Value("${ecs.cofaceLEParentFileName}")
    private String cofaceLEParentFileName;
    @Value("${ecs.cofaceLEChildFileName}")
    private String cofaceLEChildFileName;
    @Value("${ecs.cofaceIncapablesFileName}")
    private String cofaceIncapablesFileName;
    @Value("${ecs.cofaceLEDeleteFileName:COFACE_LE_DEL.csv}")
    private String cofaceLEDeleteFileName;
    @Value("${ecs.readFileFrom}")
    private String readFileFrom;
    @Value("${app.input-file}")
    private Resource resourceOUFile;
    @Value("${app.le-parent-file}")
    private Resource resourceLEParentFile;
    @Value("${app.le-child-file}")
    private Resource resourceLEChildFile;
    @Value("${app.incapables-file}")
    private Resource resourceIncapablesFile;
    @Value("${app.le-delete-file:classpath:COFACE_LE_DEL.csv}")
    private Resource resourceLEDeleteFile;

    @Bean
    @StepScope
    public FlatFileItemReader<OrganisationUnitDomain> organisationUnitReader() {
        String fileId = StepSynchronizationManager.getContext().getStepExecution()
                .getJobParameters().getString("opsFileId");
        return new FlatFileItemReaderBuilder<OrganisationUnitDomain>()
                .name(COFACE_OPS_BATCH_FILE_READER_NAME)
                .resource(getFileForLocalRun(readFileFrom, cofaceOUFileName,fileId))
                .delimited()
                .names(OrganisationUnit.LE_EXTERNAL_IDENTIFIER, OPS_EXTERNAL_IDENTIFIER_VAL, ORGANISATION_UNIT_NAME,
                        ADDRESS, UNIT_POSTAL_ADDRESS_STREET_NM, UNIT_POSTAL_ADDRESS_HOUSE_NUM,
                        UNIT_POSTAL_ADDRESS_HOUSE_ADD, UNIT_POSTAL_ADDRESS_POSTAL_CD,
                        UNIT_POSTAL_ADDRESS_CITY_NAME, UNIT_POSTAL_ADDRESS_CNTRY_CD,
                        OPS_EXT_ID_DATE_OF_ISSUE, OPS_EXT_ID_EXPIRY_DATE,
                        UNIT_DIGITAL_ADDR_FULL_TEL,
                        UNIT_DIGITAL_ADDR_EMAIL)
                .strict(false)
                .fieldSetMapper(new OrganisationUnitFieldSetMapper())
                .linesToSkip(1)
                .build();
    }

    @Bean
    public OrganisationUnitEnrichmentProcessor organisationUnitProcessor() {
        return new OrganisationUnitEnrichmentProcessor(mappingDAO, taskletCofaceOpsDomainWrapper);
    }


    @Bean
    public JdbcBatchItemWriter<OrganisationUnitDomainWrapper> organisationUnitWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<OrganisationUnitDomainWrapper>()
                .sql(COFACE_OPS_TABLE_INSERT_SQL)
                .beanMapped()
                .dataSource(dataSource)
                .build();
    }


    @Bean
    public Step organisationUnitEnrichmentStep(JobRepository jobRepository,
                                               PlatformTransactionManager transactionManager,
                                               FlatFileItemReader<OrganisationUnitDomain> organisationUnitReader,
                                               OrganisationUnitEnrichmentProcessor organisationUnitProcessor,
                                               JdbcBatchItemWriter<OrganisationUnitDomainWrapper> organisationUnitWriter) {
        return new StepBuilder(COFACE_OPS_BATCH_STEP_NAME, jobRepository)
                .<OrganisationUnitDomain, OrganisationUnitDomainWrapper>chunk(100, transactionManager)
                .reader(organisationUnitReader())
                .processor(organisationUnitProcessor)
                .writer(organisationUnitWriter)
                .listener((StepExecutionListener) organisationUnitProcessor)
                .listener(new ChunkListener() {
                    @Override
                    public void afterChunk(ChunkContext context) {
                        organisationUnitProcessor.flushNewMappings();
                    }
                })
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(Integer.MAX_VALUE)
                .listener(new org.springframework.batch.core.listener.SkipListenerSupport<OrganisationUnitDomain, OrganisationUnitDomainWrapper>() {
                    @Override
                    public void onSkipInRead(Throwable t) {
                        if (t instanceof FlatFileParseException) {
                            FlatFileParseException ex = (FlatFileParseException) t;
                            log.error("CSV Parsing error at line: {}, input: {}", ex.getLineNumber(), ex.getInput());

                            try {
                                errorLogDAO.logParsingError(
                                        RecordType.ORGANISATION_UNIT,
                                        "cofaceOpsData.csv",
                                        "Parsing error at line: " + ex.getLineNumber() + ", input: " + ex.getInput(),
                                        null,  // batchId
                                        ex.toString(),  // stackTrace
                                        null  // fileId
                                );
                            } catch (Exception logEx) {
                                log.error("Failed to log parsing error", logEx);
                            }
                        }
                    }
                })
                .retryLimit(3)
                .retry(Exception.class)
                .noRetry(FlatFileParseException.class)
                .build();
    }

    @Bean
    public Step organisationUnitSearchApiTaskletStep(JobRepository jobRepository,
                                                     PlatformTransactionManager transactionManager,
                                                     OrganisationUnitSearchApiTasklet organisationUnitSearchApiTasklet) {
        return new StepBuilder("organisationUnitSearchApiTaskletStep", jobRepository)
                .tasklet(organisationUnitSearchApiTasklet, transactionManager)
                .build();
    }

    @Bean(name = "organisationUnitEnrichmentJob")

    public Job enrichmentJob(JobRepository jobRepository, Step organisationUnitEnrichmentStep, Step organisationUnitSearchApiTaskletStep) {
        return new JobBuilder(COFACE_OPS_BATCH_JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())


                .start(organisationUnitEnrichmentStep)
                .next(organisationUnitSearchApiTaskletStep)
                .build();
    }

    // Legal Entity batch config start ----------------------------------------------------------------------------------------
    @Bean
    public Step organisationSearchApiTaskletStep(JobRepository jobRepository,
                                                 PlatformTransactionManager transactionManager,
                                                 OrganisationSearchApiTasklet organisationSearchApiTasklet) {
        return new StepBuilder("organisationSearchApiTaskletStep", jobRepository)
                .tasklet(organisationSearchApiTasklet, transactionManager)
                .build();
    }

    @Bean
    public Step legalEntityParentEnrichStep(JobRepository jobRepository,
                                            PlatformTransactionManager transactionManager,
                                            FlatFileItemReader<LegalEntityParentDomain> legalEntityParentReader,
                                            LEParentEnrichmentProcessor leParentEnrichmentProcessor,
                                            JdbcBatchItemWriter<LegalEntityParentDomain> legalEntityParentWriter) {

        return new StepBuilder(COFACE_LE_BATCH_PARENT_STEP_NAME, jobRepository)
                .<LegalEntityParentDomain, LegalEntityParentDomain>chunk(100, transactionManager)
                .reader(legalEntityParentReader())
                .processor(leParentEnrichmentProcessor)
                .writer(legalEntityParentWriter)
                .listener(new ChunkListener() {
                    @Override
                    public void afterChunk(ChunkContext context) {
                        leParentEnrichmentProcessor.flushNewMappings();
                    }
                })
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(100)
                .retryLimit(3)
                .retry(Exception.class)
                .noRetry(FlatFileParseException.class)
                .build();
    }

    @Bean
    public LegalEntityChildEnrichmentProcessor legalEntityChildEnrichmentProcessor(MappingDAO mappingDAO) {
        return new LegalEntityChildEnrichmentProcessor(mappingDAO,taskletCofaceChildDomainWrapper,errorLogDAO);
    }

    @Bean
    public Step legalEntityChildEnrichStep(JobRepository jobRepository,
                                           PlatformTransactionManager transactionManager,
                                           FlatFileItemReader<LegalEntityChildDomain> legalEntityChildReader,
                                           LegalEntityChildEnrichmentProcessor legalEntityChildEnrichmentProcessor,
                                           JdbcBatchItemWriter<LegalEntityChildDomain> legalEntityChildWriter) {

        return new StepBuilder("legalEntityChildEnrichStep", jobRepository)
                .<LegalEntityChildDomain, LegalEntityChildDomain>chunk(100, transactionManager)
                .reader(legalEntityChildReader())
                .processor(legalEntityChildEnrichmentProcessor)
                .writer(legalEntityChildWriter)
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(100)
                .retryLimit(3)
                .retry(Exception.class)
                .noRetry(FlatFileParseException.class)
                .build();
    }


    @Bean
    public LEParentEnrichmentProcessor leParentEnrichmentProcessor() {
        return new LEParentEnrichmentProcessor(mappingDAO, taskletCofaceParentDomainWrapper);
    }

    @StepScope
    @Bean
    public FlatFileItemReader<LegalEntityParentDomain> legalEntityParentReader() {
        String fileId = StepSynchronizationManager.getContext().getStepExecution()
                .getJobParameters().getString("leParentFileId");
        return new FlatFileItemReaderBuilder<LegalEntityParentDomain>()
                .name(COFACE_LE_BATCH_PARENT_FILE_READER_NAME)
                .resource(getFileForLocalRun(readFileFrom, cofaceLEParentFileName,fileId))
                .delimited()
                .names(LegalEntityParent.LE_EXTERNAL_IDENTIFIER, ORG_STATUS, ORG_NAME, ORG_OTHER_NAME,
                        ORG_OTHER_NAME_TYPE, ADR_UNSTRUCTURED_ADDRESS, LE_POSTAL_ADDRESS_STREET_NAME,
                        LE_POSTAL_ADDRESS_HOUSE_NUM, LE_POSTAL_ADDRESS_HOUSE_NUM_ADD,
                        LE_POSTAL_ADDRESS_POSTAL_CODE, LE_POSTAL_ADDRESS_CITY_NAME,
                        LE_POSTAL_ADDRESS_CNTRY_CD, ORG_LEGAL_FORM, ORG_BUSINESS_CLOSEDOWN_DATE,
                        LE_LEGAL_STATUS, ASMT_ID_VERIFY_APPROVAL_DATE, LE_PREFERRED_LANGUAGE,
                        LE_DIGITAL_ADDR_FULL_TEL, NSSO_EXTERNAL_IDENTIFIER, ASMT_VAT_STATUS,
                        NSSO_EXTERNAL_IDENTIFIER_STATUS)
                .strict(false)
                .fieldSetMapper(new LegalEntityParentFieldSetMapper())
                .linesToSkip(1)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<LegalEntityParentDomain> legalEntityParentWriter(DataSource dataSource) {

        return new JdbcBatchItemWriterBuilder<LegalEntityParentDomain>()
                .sql(LEGAL_ENTITY_PARENT_TABLE_INSERT_SQL)
                .beanMapped()
                .dataSource(dataSource)
                .build();
    }

    @StepScope
    @Bean
    public FlatFileItemReader<LegalEntityChildDomain> legalEntityChildReader() {
        String fileId = StepSynchronizationManager.getContext().getStepExecution()
                .getJobParameters().getString("leChildFileId");
        return new FlatFileItemReaderBuilder<LegalEntityChildDomain>()
                .name(COFACE_LE_BATCH_CHILD_FILE_READER_NAME)
                .resource(getFileForLocalRun(readFileFrom, cofaceLEChildFileName,fileId))
                .delimited()
                .names(LegalEntityParent.LE_EXTERNAL_IDENTIFIER, INDUSTRY_CLASS_CODE, INDUSTRY_CLASS_RANK,
                        INDUSTRY_CLASS_EFF_DATE, INDUSTRY_CLASS_END_DATE)
                .fieldSetMapper(new LegalEntityChildFieldSetMapper())
                .linesToSkip(1)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<LegalEntityChildDomain> legalEntityChildWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<LegalEntityChildDomain>()
                .sql(LEGAL_ENTITY_CHILD_TABLE_INSERT_SQL)
                .beanMapped()
                .dataSource(dataSource)
                .build();
    }

    @Bean(name = "LegalEntityEnrichmentJob")
    public Job lEEnrichmentJob(JobRepository jobRepository, Step legalEntityParentEnrichStep, Step legalEntityChildEnrichStep, Step organisationSearchApiTaskletStep) {

        return new JobBuilder(COFACE_LE_BATCH_JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(legalEntityParentEnrichStep)
                .next(legalEntityChildEnrichStep)
                .next(organisationSearchApiTaskletStep)
                .build();
    }
    // Legal Entity batch config End ----------------------------------------------------------------------------------------


    @Bean
    @StepScope
    public FlatFileItemReader<IncapableRelationship> incapablesReader() {

        String fileId = StepSynchronizationManager.getContext()
                .getStepExecution()
                .getJobParameters()
                .getString("incapablesFileId");

        return new FlatFileItemReaderBuilder<IncapableRelationship>()
                .name(INCAPABLES_BATCH_FILE_READER_NAME)
                .resource(getFileForLocalRun(readFileFrom, cofaceIncapablesFileName, fileId))
                .delimited()
                .names(INCP_GENDER, INCP_LASTNAME, INCP_FIRSTNAME, INCP_STREETNAME,
                        INCP_HOUSENUMBER, INCP_HOUSENUMBERADDITION, INCP_POSTALCODE,
                        INCP_CITYNAME, INCP_COUNTRYOFRESIDENCE, INCP_DATEOFBIRTH,
                        INCP_DATEOFDEATH, INCP_CITYOFBIRTH, INCP_COUNTRYOFBIRTH,
                        OBJECT_CODE, INAB_ENDDATE, INAB_EFFECTIVEDATE,
                        ADM_OCCUPATIONCODE, ADM_LASTNAME, ADM_FIRSTNAME,
                        ADM_STREETNAME, ADM_HOUSENUMBER, ADM_HOUSENUMBERADDITION,
                        ADM_POSTALCODE, ADM_CITYNAME, ADM_COUNTRYOFRESIDENCE,
                        ADM_RESPONSIBILITY_ENDDATE)
                .fieldSetMapper(new IncapablesFieldSetMapper())
                .linesToSkip(1)
                .build();
    }

    @Bean
    public IncapablesEnrichmentProcessor incapablesProcessor() {
        return new IncapablesEnrichmentProcessor(mappingDAO);
    }

    @Bean
    public JdbcBatchItemWriter<IncapableRelationship> incapablesWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<IncapableRelationship>()
                .sql(IncapablesQueries.INCAPABLES_TABLE_INSERT_SQL)
                .beanMapped()
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Step incapablesEnrichmentStep(JobRepository jobRepository,
                                         PlatformTransactionManager transactionManager,
                                         FlatFileItemReader<IncapableRelationship> incapablesReader,
                                         IncapablesEnrichmentProcessor incapablesProcessor,
                                         JdbcBatchItemWriter<IncapableRelationship> incapablesWriter) {
        return new StepBuilder(INCAPABLES_BATCH_STEP_NAME, jobRepository)
                .<IncapableRelationship, IncapableRelationship>chunk(100, transactionManager)
                .reader(incapablesReader)
                .processor(incapablesProcessor)
                .writer(incapablesWriter)
                .listener(incapablesProcessor)
                .listener(new ChunkListener() {
                    @Override
                    public void afterChunk(ChunkContext context) {
                        incapablesProcessor.flushNewMappings();
                    }
                })
                .faultTolerant()
                .retryLimit(3)
                .retry(Exception.class)
                .build();
    }

    @Bean(name = "IncapablesEnrichmentJob")
    public Job incapablesEnrichmentJob(JobRepository jobRepository, Step incapablesEnrichmentStep) {
        log.info("IncapablesEnrichmentJob job started");
        return new JobBuilder(INCAPABLES_BATCH_JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(incapablesEnrichmentStep)
                .build();
    }

    // Legal Entity Deletion batch config start ----------------------------------------------------------------------------------------

    @Bean
    @StepScope
    public FlatFileItemReader<LegalEntityDeletionDomain> legalEntityDeletionReader() {
        String fileId = StepSynchronizationManager.getContext().getStepExecution()
                .getJobParameters().getString("leDeletionFileId");
        return new FlatFileItemReaderBuilder<LegalEntityDeletionDomain>()
                .name(COFACE_LE_DELETION_BATCH_FILE_READER_NAME)
                .resource(getFileForLocalRun(readFileFrom, cofaceLEDeleteFileName, fileId))
                .delimited()
                .names(LegalEntityDeletionFieldSetMapper.LE_EXTERNAL_IDENTIFIER,
                        LegalEntityDeletionFieldSetMapper.LE_DELETION_FLAG)
                .fieldSetMapper(new LegalEntityDeletionFieldSetMapper())
                .linesToSkip(1)
                .build();
    }

    @Bean
    public LEDeletionEnrichmentProcessor leDeletionEnrichmentProcessor() {
        return new LEDeletionEnrichmentProcessor(mappingDAO, legalEntityDeletionDAO, taskletLeDeletionDomainWrapper, errorLogDAO);
    }

    @Bean
    public JdbcBatchItemWriter<LegalEntityDeletionDomain> legalEntityDeletionWriter(DataSource dataSource) {
        String sql = "INSERT INTO DD_LEGALENTITY_DELETE_TBL (LE_DELETION_FLAG, DD_UUID, FILE_ID, CREATED_BY, CREATED_TS) " +
                "VALUES (:leDeletionFlag, :ddUuid, :fileId, 'DD_USER', SYSTIMESTAMP)";
        return new JdbcBatchItemWriterBuilder<LegalEntityDeletionDomain>()
                .sql(sql)
                .beanMapped()
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Step legalEntityDeletionEnrichStep(JobRepository jobRepository,
                                              PlatformTransactionManager transactionManager,
                                              FlatFileItemReader<LegalEntityDeletionDomain> legalEntityDeletionReader,
                                              LEDeletionEnrichmentProcessor leDeletionEnrichmentProcessor,
                                              JdbcBatchItemWriter<LegalEntityDeletionDomain> legalEntityDeletionWriter) {
        return new StepBuilder(COFACE_LE_DELETION_BATCH_STEP_NAME, jobRepository)
                .<LegalEntityDeletionDomain, LegalEntityDeletionDomain>chunk(100, transactionManager)
                .reader(legalEntityDeletionReader())
                .processor(leDeletionEnrichmentProcessor)
                .writer(legalEntityDeletionWriter)
                .listener((StepExecutionListener) leDeletionEnrichmentProcessor)
                .listener(new ChunkListener() {
                    @Override
                    public void afterChunk(ChunkContext context) {
                        leDeletionEnrichmentProcessor.flushRecords();
                    }
                })
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skipLimit(100)
                .retryLimit(3)
                .retry(Exception.class)
                .noRetry(FlatFileParseException.class)
                .build();
    }

    @Bean
    public Step leDeletionApiTaskletStep(JobRepository jobRepository,
                                         PlatformTransactionManager transactionManager,
                                         LEDeletionApiTasklet leDeletionApiTasklet) {
        return new StepBuilder(LE_DELETION_API_TASKLET_STEP, jobRepository)
                .tasklet(leDeletionApiTasklet, transactionManager)
                .build();
    }

    @Bean(name = "LegalEntityDeletionJob")
    public Job legalEntityDeletionJob(JobRepository jobRepository,
                                      Step legalEntityDeletionEnrichStep,
                                      Step leDeletionApiTaskletStep) {
        log.info("LegalEntityDeletionJob job started");
        return new JobBuilder(COFACE_LE_DELETION_BATCH_JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(legalEntityDeletionEnrichStep)
                .next(leDeletionApiTaskletStep)
                .build();
    }
    // Legal Entity Deletion batch config End ----------------------------------------------------------------------------------------


    // Read File from ECS/Local-folder --------------------------------------------------------------------------------------
    private Resource getFileForLocalRun(String fileFrom, String fileName, String fileId) {
        Resource resource = null;
        log.info("DD_Data_Distributor Read File from {}, filename:{}", fileFrom, fileName);
        if (fileFrom.equalsIgnoreCase("ECS")) {
            resource = fileProviderService.getFile(bucketName, fileName);
        } else if (fileName.equalsIgnoreCase(cofaceLEChildFileName)) {
            resource = resourceLEChildFile;
        } else if (fileName.equalsIgnoreCase(cofaceLEParentFileName)) {
            resource = resourceLEParentFile;
        } else if (fileName.equalsIgnoreCase(cofaceOUFileName)) {
            resource = resourceOUFile;
        } else if (fileName.equalsIgnoreCase(cofaceIncapablesFileName)) {
            resource = resourceIncapablesFile;
        } else if (fileName.equalsIgnoreCase(cofaceLEDeleteFileName)) {
            resource = resourceLEDeleteFile;
        } else {
            log.error("DD_Data_Distributor Unknown File Name {}", fileName);
            return null;
        }
        log.info("DD_Data_Distributor File Downloaded Successfully...!");
        fileIngestionService.updateTotalNumberOfRecords(fileId, resource);
        return resource;
    }

    public void logDataRetentionSchedulerStatus(DataRetentionConfig dataRetentionConfig) {
        boolean isRetentionJobEnabled = dataRetentionConfig.isEnabled();
        dataRetentionConfig.logRetentionJobStatus(isRetentionJobEnabled);
    }

    public class SkipFooterLinePolicy extends DefaultRecordSeparatorPolicy {

        @Override

        public String preProcess(String record) {

            if (record == null) {

                return null;

            }

            String trimmed = record.trim();

            if (trimmed.isEmpty()) {

                return "";

            }

            if (trimmed.matches("^\\d+$")) {

                return "";

            }

            return record;

        }

    }


}


package com.ing.datadist.batch.config;

import com.ing.datadist.domain.LegalEntityChildDomain;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.lang.NonNull;

import static com.ing.datadist.domain.fileinput.LegalEntityChild.*;
import static com.ing.datadist.domain.fileinput.LegalEntityParent.LE_EXTERNAL_IDENTIFIER;

public class LegalEntityChildFieldSetMapper implements FieldSetMapper<LegalEntityChildDomain> {
    @Override
    public @NonNull LegalEntityChildDomain mapFieldSet(@NonNull FieldSet fieldSet) {
        LegalEntityChildDomain legalEntityChildDomain = new LegalEntityChildDomain();
        legalEntityChildDomain.setLeExternalIdentifier(fieldSet.readString(LE_EXTERNAL_IDENTIFIER));
        legalEntityChildDomain.setIndustryClassCode(fieldSet.readString(INDUSTRY_CLASS_CODE));
        legalEntityChildDomain.setIndustryClassRank(fieldSet.readInt(INDUSTRY_CLASS_RANK));
        legalEntityChildDomain.setIndustryClassEffDate(fieldSet.readString(INDUSTRY_CLASS_EFF_DATE));
        legalEntityChildDomain.setIndustryClassEndDate(fieldSet.readString(INDUSTRY_CLASS_END_DATE));

        return legalEntityChildDomain;
    }
}
package com.ing.datadist.batch.config;

import com.ing.datadist.domain.LegalEntityDeletionDomain;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.lang.NonNull;

/**
 * FieldSetMapper for Legal Entity Deletion CSV file
 */
public class LegalEntityDeletionFieldSetMapper implements FieldSetMapper<LegalEntityDeletionDomain> {

    public static final String LE_EXTERNAL_IDENTIFIER = "LE_ExternalIdentifier";
    public static final String LE_DELETION_FLAG = "LE_DELETIONFLAG";

    @Override
    public @NonNull LegalEntityDeletionDomain mapFieldSet(@NonNull FieldSet fieldSet) {
        LegalEntityDeletionDomain domain = new LegalEntityDeletionDomain();
        domain.setLeExternalIdentifier(fieldSet.readString(LE_EXTERNAL_IDENTIFIER));
        domain.setLeDeletionFlag(fieldSet.readString(LE_DELETION_FLAG));
        return domain;
    }
}
package com.ing.datadist.batch.config;

import com.ing.datadist.domain.LegalEntityParentDomain;
import com.ing.datadist.domain.OrganisationUnitDomain;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.lang.NonNull;

import static com.ing.datadist.domain.fileinput.OrganisationUnit.*;
@Slf4j
public class OrganisationUnitFieldSetMapper implements FieldSetMapper<OrganisationUnitDomain> {
    @Override
    public @NonNull OrganisationUnitDomain mapFieldSet(@NonNull FieldSet fieldSet) {

        OrganisationUnitDomain cofaceOpsDomain = new OrganisationUnitDomain();
        cofaceOpsDomain.setOrgExternalIdentifierVal(fieldSet.readString(LE_EXTERNAL_IDENTIFIER));
        cofaceOpsDomain.setOpsExternalIdentifierVal(fieldSet.readString(OPS_EXTERNAL_IDENTIFIER_VAL));
        cofaceOpsDomain.setOrganisationUnitName(fieldSet.readString(ORGANISATION_UNIT_NAME));
        cofaceOpsDomain.setAddress(fieldSet.readString(ADDRESS));
        cofaceOpsDomain.setPostalAddressStreetNm(fieldSet.readString(UNIT_POSTAL_ADDRESS_STREET_NM));
        cofaceOpsDomain.setPostalAddressHouseNum(fieldSet.readString(UNIT_POSTAL_ADDRESS_HOUSE_NUM));
        cofaceOpsDomain.setPostalAddressHouseAdd(fieldSet.readString(UNIT_POSTAL_ADDRESS_HOUSE_ADD));
        cofaceOpsDomain.setPostalAddressPostalCd(fieldSet.readString(UNIT_POSTAL_ADDRESS_POSTAL_CD));
        cofaceOpsDomain.setPostalAddressCityName(fieldSet.readString(UNIT_POSTAL_ADDRESS_CITY_NAME));
        cofaceOpsDomain.setPostalAddressCntryCd(fieldSet.readString(UNIT_POSTAL_ADDRESS_CNTRY_CD));
        cofaceOpsDomain.setOpsExtIdDateOfIssue(fieldSet.readString(OPS_EXT_ID_DATE_OF_ISSUE));
        cofaceOpsDomain.setOpsExtIdExpiryDate(fieldSet.readString(OPS_EXT_ID_EXPIRY_DATE));
        cofaceOpsDomain.setDigitalAddrFullTel(fieldSet.readString(UNIT_DIGITAL_ADDR_FULL_TEL));
        cofaceOpsDomain.setDigitalAddrEmail(fieldSet.readString(UNIT_DIGITAL_ADDR_EMAIL));
        return cofaceOpsDomain;
    }
}
"LE_ExternalIdentifier","LE_DELETIONFLAG"
"0568777217","Y"
"0573423814","Y"
2

incapabels
"incp_gender","incp_lastname","incp_firstname","incp_streetname","incp_housenumber","incp_housenumberaddition","incp_postalcode","incp_cityname","incp_countryofresidence","incp_dateofbirth","incp_dateofdeath","incp_cityofbirth","incp_countryofbirth","object_code","inab_enddate","inab_effectivedate","adm_occupationcode","adm_lastname","adm_firstname","adm_streetname","adm_housenumber","adm_housenumberaddition","adm_postalcode","adm_cityname","adm_countryofresidence","adm_responsibility_enddate"
"ML","Hallet","Rimbaud","Rue de Cornillon","44","0021","4020","LIEGE","BE","1998-05-15","","SERAING","BE","1001","","","","Deguel","FranÃ§ois","Rue des Vennes","91","","4020","LIEGE","BE",
"FEML","Lahaye","Jessica","Rue LÃ©on Dubois","279","1","6030","CHARLEROI","BE","1998-01-14","","MONS","BE","1002","","","160","Coudou","Laurence","Boulevard Audent","11","1","6000","CHARLEROI","BE",
"ML","Dess","Laurence","Rue LÃ©o Darton","58","","6030","CHARLEROI","BE","1969-07-16","","DINANT","BE","1002","","","160","Palate","StÃ©phanie","Boulevard Audent","11","3","6000","CHARLEROI","BE",
3



child "LE_ExternalIdentifier","IndustryClass_Code","IndustryClass_Rank","IndustryClass_EffDate","IndustryClass_EndDate"
"1017306504","84114","1","01-03-2002","12-12-2028"
1


parent
"LE_ExternalIdentifier","ORG_Status","ORG_Name","ORG_OtherName","ORG_OtherNameType","Adr_UnstructuredAddress","Adr_PostalAddress_StreetName","Adr_PostalAddress_HouseNum","Adr_PostalAddress_HouseNumAdd","Adr_PostalAddress_PostalCode","Adr_PostalAddress_CityName","Adr_PostalAddress_CountryCode","ORG_LegalForm","ORG_BusinessClosedownDate","LE_LegalStatus","ASMT_IdVerif_ApprovalDate","LE_PreferredLanguage","Adr_DigitalAddr_FullTel","NSSO_ExternalIdentifier","ASMT_VAT_Status","NSSO_ExternalIdentifierStatus"
"0607935622","AC","WOLUWE SHOPPING OPTIC","WS OPTIC","abbrev","Bd de la Woluwe 70 B.103","Bd de la Woluwe","70","103","1200","BRUXELLES","BE","015","","000","24/03/2015","1","02/771.60.65","140372094","0","1"
3


cofaceops
ExternalIdentifierVal"le_externalidentifier_val","ops_externalidentifier_val","ops_organisationunitname","address","adr_postaladdress_streetnm","adr_postaladdress_housenum","adr_postaladdress_houseadd","adr_postaladdress_postalcd","adr_postaladdress_cityname","adr_postaladdress_cntrycd","ops_extid_dateofissue","ops_extid_expirydate","adr_digitaladdr_email","adr_digitaladdr_fulltel"
"0628656505","2259391207","Fiduciaire MBL SNC","Rue de Jalhay 33 B 10","Rue de Jalhay","33","10","4801","STEMBERT","BE","01/01/2017","10/11/2017","fiduciairesandrinedonneau@gmail.com","0493199825",


package com.ing.datadist.service;

import com.ing.datadist.dao.FileIngestionDAO;
import com.ing.datadist.domain.FileIngestionDomain;
import com.ing.datadist.enums.FileIngestionStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.UUID;

@Slf4j
@Service
public class FileIngestionService {

    private final FileIngestionDAO fileIngestionDAO;

    public FileIngestionService(FileIngestionDAO fileIngestionDAO) {
        this.fileIngestionDAO = fileIngestionDAO;
    }

    /**
     * Register a new file for ingestion
     */
    public String registerFileIngestion(String fileName, String flowName, String flowType,
                                       String sourceSystem, String frequency, Long totalRecords) {
        try {
            FileIngestionDomain fileIngestion = new FileIngestionDomain();
            String fileId = generateFileId();

            fileIngestion.setFileId(fileId);
            fileIngestion.setFileName(fileName);
            fileIngestion.setFlowName(flowName);
            fileIngestion.setFlowType(flowType);
            fileIngestion.setSourceSystem(sourceSystem);
            fileIngestion.setFrequency(frequency);
            fileIngestion.setStatus(FileIngestionStatus.TO_BE_PROCESSED.getValue());
            fileIngestion.setTotalRecords(totalRecords != null ? totalRecords : 0L);
            fileIngestion.setProcessedRecords(0L);
            fileIngestion.setFailedRecords(0L);
            fileIngestion.setSkippedRecords(0L);
            fileIngestion.setRetryCount(0);
            fileIngestion.setCreatedBy("DD_SYSTEM");

            fileIngestionDAO.insertFileIngestion(fileIngestion);
            log.info("File Ingestion Service: Registered file - fileId: {}, fileName: {}, flowName: {}, totalRecords: {}",
                    fileId, fileName, flowName, totalRecords);

            return fileId;
        } catch (Exception e) {
            log.error("File Ingestion Service: Error registering file - fileName: {}, error: {}",
                    fileName, e.getMessage());
            throw new RuntimeException("Failed to register file ingestion", e);
        }
    }

    /**
     * Update file status to IN_PROGRESS
     */
    public void markFileProcessingStart(String fileId, String batchId) {
        try {
            fileIngestionDAO.updateProcessingStart(fileId, batchId);
            log.info("File Ingestion Service: Marked processing start - fileId: {}, batchId: {}", fileId, batchId);
        } catch (Exception e) {
            log.error("File Ingestion Service: Error marking processing start - fileId: {}, error: {}",
                    fileId, e.getMessage());
            throw new RuntimeException("Failed to mark file processing start", e);
        }
    }

    /**
     * Update file status to COMPLETED with metrics
     */
    public void markFileProcessingComplete(String fileId,
                                           Long totalRecords,
                                           Long processedRecords,
                                           Long failedRecords,
                                           Long fileSize,
                                           String fileChecksum) {

        fileIngestionDAO.updateFileProcessingEnd(
                fileId,
                fileSize,
                fileChecksum,
                totalRecords,
                processedRecords,
                failedRecords
        );

        log.info("File Ingestion: COMPLETE - fileId={}, total={}, processed={}, failed={}",
                fileId, totalRecords, processedRecords, failedRecords);
    }
    /**
     * Update file processing metrics during processing
     */
    public void updateFileProcessingMetrics(String fileId, Long processedRecords, Long failedRecords, Long skippedRecords) {
        try {
            fileIngestionDAO.updateFileMetrics(fileId, processedRecords, failedRecords, skippedRecords);
            log.info("File Ingestion Service: Updated processing metrics - fileId: {}, processed: {}, failed: {}, skipped: {}",
                    fileId, processedRecords, failedRecords, skippedRecords);
        } catch (Exception e) {
            log.error("File Ingestion Service: Error updating processing metrics - fileId: {}, error: {}",
                    fileId, e.getMessage());
            throw new RuntimeException("Failed to update file processing metrics", e);
        }
    }

    /**
     * Mark file as FAILED with error message
     */
    public void markFileProcessingFailed(String fileId, String errorMessage) {

        fileIngestionDAO.updateFileIngestionStatus(
                fileId,
                FileIngestionStatus.FAILED.getValue(),
                null,       // keep TOTAL_RECORDS
                null,       // keep PROCESSED_RECORDS
                null,       // keep FAILED_RECORDS
                errorMessage
        );

        log.info("File Ingestion Service: Marked processing failed - fileId: {}, error: {}",
                fileId, errorMessage);
    }    /**
     * Update file processing end with file metadata and final metrics
     */
    public void updateFileProcessingEnd(String fileId, Long fileSize, String fileChecksum,
                                       Long totalRecords, Long processedRecords, Long failedRecords) {
        try {
            fileIngestionDAO.updateFileProcessingEnd(fileId, fileSize, fileChecksum,
                    totalRecords, processedRecords, failedRecords);
            log.info("File Ingestion Service: Updated processing end - fileId: {}, fileSize: {}, " +
                    "fileChecksum: {}, totalRecords: {}, processedRecords: {}, failedRecords: {}",
                    fileId, fileSize, fileChecksum, totalRecords, processedRecords, failedRecords);
        } catch (Exception e) {
            log.error("File Ingestion Service: Error updating processing end - fileId: {}, error: {}",
                    fileId, e.getMessage());
            throw new RuntimeException("Failed to update file processing end", e);
        }
    }

    /**
     * Get file ingestion record by fileId
     */
    public FileIngestionDomain getFileIngestion(String fileId) {
        try {
            return fileIngestionDAO.getFileIngestionByFileId(fileId);
        } catch (Exception e) {
            log.error("File Ingestion Service: Error retrieving file ingestion - fileId: {}, error: {}",
                    fileId, e.getMessage());
            throw new RuntimeException("Failed to retrieve file ingestion", e);
        }
    }

    /**
     * Generate unique file ID
     */
    private String generateFileId() {
        return "FILE_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString().substring(0, 8);
    }
    public void updateTotalNumberOfRecords(String fileId, Resource resource) {
        try {
            long totalRecords = 0L;
            long fileSize = resource.contentLength();

            // Try to read the last line and parse it as a number (footer count)
            String lastLine;
            try (BufferedReader br = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
                String line;
                String tmp = null;
                while ((line = br.readLine()) != null) {
                    tmp = line;
                }
                lastLine = tmp;
            }

            if (lastLine != null) {
                String cleaned = lastLine.replace(",", "").replace("\"", "").trim();
                if (!cleaned.isEmpty() && cleaned.matches("^[0-9]+$")) {
                    totalRecords = Long.parseLong(cleaned);
                    log.info("File Ingestion Service: footer-based totalRecords detected: {} (fileId: {})", totalRecords, fileId);
                }
            }

            // Fallback if no numeric footer found: count data rows
            if (totalRecords == 0L) {
                long lines = 0L;
                long numericLikeLines = 0L;
                try (BufferedReader br = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        lines++;
                        String cleaned = line.replace(",", "").replace("\"", "").trim();
                        if (cleaned.matches("^[0-9]+$")) {
                            numericLikeLines++;
                        }
                    }
                }
                // lines = header + data + possible numeric-like lines (footer or stray)
                // data = lines - header(1) - numericLikeLines
                totalRecords = Math.max(lines - 1 - numericLikeLines, 0L);
                log.info("File Ingestion Service: counted totalRecords without footer: {} (lines={}, numericLikeLines={}, fileId={})",
                        totalRecords, lines, numericLikeLines, fileId);
            }

            fileIngestionDAO.updateTotalNumberOfRecords(fileId, totalRecords, fileSize);

        } catch (Exception e) {
            log.error("File Ingestion Service: Error while updating TotalNumberOfRecords - fileId: {}, error: {}",
                    fileId, e.getMessage(), e);
        }
    }
}
package com.ing.datadist.util;

import lombok.extern.slf4j.Slf4j;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Utility class to parse total records count from CSV files
 * The total records count is expected to be in the last line of the file
 */
@Slf4j
public class CsvTotalRecordsParser {

    /**
     * Read total records count from CSV file (last line)
     * @param filePath Path to the CSV file
     * @return Total records count, or 0 if not found
     */
    public static Long getTotalRecordsFromCsv(String filePath) {
        try {
            String lastLine = getLastLineFromFile(filePath);
            if (lastLine != null && !lastLine.trim().isEmpty()) {
                try {
                    Long totalRecords = Long.parseLong(lastLine.trim());
                    log.info("CsvTotalRecordsParser: Found total records {} from file {}", totalRecords, filePath);
                    return totalRecords;
                } catch (NumberFormatException e) {
                    log.warn("CsvTotalRecordsParser: Last line is not a number: {}", lastLine);
                    return 0L;
                }
            }
        } catch (Exception e) {
            log.error("CsvTotalRecordsParser: Error reading total records from file {}, error: {}", filePath, e.getMessage());
        }
        return 0L;
    }

    /**
     * Get the last line from a file
     * @param filePath Path to the file
     * @return Last line content or null if file is empty
     */
    private static String getLastLineFromFile(String filePath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            String lastLine = null;
            while ((line = br.readLine()) != null) {
                lastLine = line;
            }
            return lastLine;
        }
    }

    /**
     * Get file size in bytes
     * @param filePath Path to the file
     * @return File size in bytes
     */
    public static Long getFileSizeInBytes(String filePath) {
        try {
            return Files.size(Paths.get(filePath));
        } catch (IOException e) {
            log.error("CsvTotalRecordsParser: Error getting file size for {}, error: {}", filePath, e.getMessage());
            return 0L;
        }
    }
}
package com.ing.datadist.util;

import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;

/**
 * FileUtility - Utility class for file operations
 * Handles file checksum calculation and file size retrieval
 */
@Slf4j
public class FileUtility {

    /**
     * Calculate MD5 checksum of a file
     * @param filePath Path to the file
     * @return MD5 checksum hex string
     */
    public static String calculateMD5Checksum(String filePath) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[8192];
            int bytesRead;

            try (FileInputStream fis = new FileInputStream(filePath)) {
                while ((bytesRead = fis.read(buffer)) != -1) {
                    md.update(buffer, 0, bytesRead);
                }
            }

            byte[] digest = md.digest();
            StringBuilder hexString = new StringBuilder();
            for (byte b : digest) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (Exception e) {
            log.error("Error calculating MD5 checksum for file: {}", filePath, e);
            return null;
        }
    }

    /**
     * Get file size in bytes
     * @param filePath Path to the file
     * @return File size in bytes
     */
    public static Long getFileSize(String filePath) {
        try {
            return Files.size(Paths.get(filePath));
        } catch (IOException e) {
            log.error("Error getting file size for file: {}", filePath, e);
            return 0L;
        }
    }

    /**
     * Get file size in bytes from File object
     * @param file File object
     * @return File size in bytes
     */
    public static Long getFileSize(File file) {
        try {
            if (file != null && file.exists()) {
                return file.length();
            }
            return 0L;
        } catch (Exception e) {
            log.error("Error getting file size for file: {}", file, e);
            return 0L;
        }
    }

    /**
     * Format file size from bytes to human-readable format (KB, MB, GB)
     * @param bytes File size in bytes
     * @return Formatted file size string
     */
    public static String formatFileSize(Long bytes) {
        if (bytes == null || bytes <= 0) return "0 B";

        final String[] units = new String[]{"B", "KB", "MB", "GB", "TB"};
        int unitIndex = 0;
        double size = bytes.doubleValue();

        while (size >= 1024 && unitIndex < units.length - 1) {
            size /= 1024.0;
            unitIndex++;
        }

        return String.format("%.2f %s", size, units[unitIndex]);
    }

    /**
     * Check if file exists
     * @param filePath Path to the file
     * @return true if file exists, false otherwise
     */
    public static boolean fileExists(String filePath) {
        try {
            return Files.exists(Paths.get(filePath));
        } catch (Exception e) {
            log.error("Error checking file existence: {}", filePath, e);
            return false;
        }
    }
}
package com.ing.datadist.batch.processor;

import com.ing.datadist.api.utils.CompositeKeyGenerator;
import com.ing.datadist.dao.FlowType;
import com.ing.datadist.dao.MappingDAO;
import com.ing.datadist.domain.MappingTableRecord;
import com.ing.datadist.domain.incapables.IncapableRelationship;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;

import java.util.*;

@Slf4j
@Getter
@StepScope
public class IncapablesEnrichmentProcessor implements ItemProcessor<IncapableRelationship, IncapableRelationship>, StepExecutionListener {

    private final MappingDAO dao;
    private final List<MappingTableRecord> newMappingTableRecords = new ArrayList<>();
    private Map<String, String> incapableUuidMap;

    public IncapablesEnrichmentProcessor(MappingDAO dao) {
        this.dao = dao;
    }

    @Override
    public IncapableRelationship process(IncapableRelationship item) throws Exception {


        String incapableCompositeKey = CompositeKeyGenerator.generateIncapableCompositeKey(
                item.getIncapablePerson().getFirstName(),
                item.getIncapablePerson().getLastName(),
                item.getIncapablePerson().getPostalAddress() != null ?
                    item.getIncapablePerson().getPostalAddress().getHouseNumber() : null,
                item.getIncapablePerson().getPostalAddress() != null ?
                    item.getIncapablePerson().getPostalAddress().getPostalCode() : null,
                item.getIncapablePerson().getDateOfBirth()
        );

        // Check if incapable person UUID exists
        String incapableUUID = incapableUuidMap.get(incapableCompositeKey);
        if (incapableUUID == null) {
            // New incapable person - create UUID and mapping record
            incapableUUID = UUID.randomUUID().toString();
            incapableUuidMap.put(incapableCompositeKey, incapableUUID);

            MappingTableRecord incapableRecord = MappingTableRecord.builder()
                    .ddUuid(incapableUUID)
                    .compositeKey(incapableCompositeKey)
                    .typeOfEntity(FlowType.INCAPABLE.getLabel())
                    .firstName(item.getIncapablePerson().getFirstName())
                    .lastName(item.getIncapablePerson().getLastName())
                    .streetName(item.getIncapablePerson().getPostalAddress() != null ?
                               item.getIncapablePerson().getPostalAddress().getStreetName() : null)
                    .houseNumber(item.getIncapablePerson().getPostalAddress() != null ?
                                item.getIncapablePerson().getPostalAddress().getHouseNumber() : null)
                    .postalCode(item.getIncapablePerson().getPostalAddress() != null ?
                               item.getIncapablePerson().getPostalAddress().getPostalCode() : null)
                    .birthDate(item.getIncapablePerson().getDateOfBirth())
                    .build();

            newMappingTableRecords.add(incapableRecord);
            log.info("Created new mapping record for incapable person: UUID={}, compositeKey={}", incapableUUID, incapableCompositeKey);
        } else {
            log.debug("Found existing UUID={} for incapable person with composite key {}", incapableUUID, incapableCompositeKey);
        }

        item.setDdUuid(incapableUUID);

        // Generate composite key for administrator
        String administratorCompositeKey = CompositeKeyGenerator.generateAdministratorCompositeKey(
                item.getAdministrator().getFirstName(),
                item.getAdministrator().getLastName(),
                item.getAdministrator().getPostalAddress() != null ?
                    item.getAdministrator().getPostalAddress().getHouseNumber() : null,
                item.getAdministrator().getPostalAddress() != null ?
                    item.getAdministrator().getPostalAddress().getPostalCode() : null,
                item.getAdministrator().getOccupationCode()
        );

        // Check if administrator UUID exists
        String administratorUUID = incapableUuidMap.get(administratorCompositeKey);
        if (administratorUUID == null) {
            // New administrator - create UUID and mapping record
            administratorUUID = UUID.randomUUID().toString();
            incapableUuidMap.put(administratorCompositeKey, administratorUUID);

            MappingTableRecord adminRecord = MappingTableRecord.builder()
                    .ddUuid(administratorUUID)
                    .compositeKey(administratorCompositeKey)
                    .typeOfEntity(FlowType.INCAPABLE.getLabel())
                    .firstName(item.getAdministrator().getFirstName())
                    .lastName(item.getAdministrator().getLastName())
                    .streetName(item.getAdministrator().getPostalAddress() != null ?
                               item.getAdministrator().getPostalAddress().getStreetName() : null)
                    .houseNumber(item.getAdministrator().getPostalAddress() != null ?
                                item.getAdministrator().getPostalAddress().getHouseNumber() : null)
                    .postalCode(item.getAdministrator().getPostalAddress() != null ?
                               item.getAdministrator().getPostalAddress().getPostalCode() : null)
                    .birthDate(null) // Administrator has no birth date
                    .build();

            newMappingTableRecords.add(adminRecord);
            log.info("Created new mapping record for administrator: UUID={}, compositeKey={}", administratorUUID, administratorCompositeKey);
        } else {
            log.debug("Found existing UUID={} for administrator with composite key {}", administratorUUID, administratorCompositeKey);
        }

        item.setAdministratorDdUuid(administratorUUID);

        return item;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info("Preloading incapable flow mapping table into memory");
        incapableUuidMap = dao.fetchAllIncapableFlowUuidMappings();
        log.info("Loaded {} UUIDs from mapping table for incapable flow", incapableUuidMap.size());
    }


    public void flushNewMappings() {
        if (!newMappingTableRecords.isEmpty()) {
            log.info("Flushing {} new mapping records to DB", newMappingTableRecords.size());
            dao.batchInsertMappingRecords(newMappingTableRecords);
            newMappingTableRecords.clear();
        }
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {

        long processed = stepExecution.getWriteCount();
        long failed = stepExecution.getProcessSkipCount();

        stepExecution.getJobExecution().getExecutionContext().put("totalRecords", processed + failed);
        stepExecution.getJobExecution().getExecutionContext().put("processedRecords", processed);
        stepExecution.getJobExecution().getExecutionContext().put("failedRecords", failed);

        flushNewMappings();

        return ExitStatus.COMPLETED;
    }
}

package com.ing.datadist.batch.processor;

import com.ing.datadist.dao.ErrorLogDAO;
import com.ing.datadist.dao.LegalEntityDeletionDAO;
import com.ing.datadist.dao.MappingDAO;
import com.ing.datadist.domain.LegalEntityDeletionDomain;
import com.ing.datadist.domain.TaskletLeDeletionDomainWrapper;
import com.ing.datadist.enums.RecordType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Batch processor for Legal Entity Deletion flow
 * Similar to LEParentEnrichmentProcessor
 */
@Slf4j
public class LEDeletionEnrichmentProcessor implements ItemProcessor<LegalEntityDeletionDomain, LegalEntityDeletionDomain>, StepExecutionListener {

    private final MappingDAO mappingDAO;
    private final LegalEntityDeletionDAO deletionDAO;
    private final TaskletLeDeletionDomainWrapper taskletWrapper;
    private final ErrorLogDAO errorLogDAO;

    private Map<String, String> leUuidMap;
    private String fileId;
    private final List<LegalEntityDeletionDomain> recordsToInsert = new ArrayList<>();

    public LEDeletionEnrichmentProcessor(MappingDAO mappingDAO,
                                         LegalEntityDeletionDAO deletionDAO,
                                         TaskletLeDeletionDomainWrapper taskletWrapper,
                                         ErrorLogDAO errorLogDAO) {
        this.mappingDAO = mappingDAO;
        this.deletionDAO = deletionDAO;
        this.taskletWrapper = taskletWrapper;
        this.errorLogDAO = errorLogDAO;
    }

    @Override
    public LegalEntityDeletionDomain process(LegalEntityDeletionDomain item) throws Exception {
        item.setFileId(this.fileId);

        // Validate LE External Identifier
        if (item.getLeExternalIdentifier() == null || item.getLeExternalIdentifier().trim().isEmpty()) {
            log.warn("Data_Distributor_LE_Deletion Skipping record with empty LE External Identifier");
            return null;
        }

        // Validate deletion flag - if null or invalid, default to 'N' and still save
        String deletionFlag = item.getLeDeletionFlag();
        if (deletionFlag == null || deletionFlag.trim().isEmpty()) {
            log.warn("Data_Distributor_LE_Deletion Record with empty deletion flag, defaulting to 'N' for LE_EXTERNAL_IDENTIFIER: {}",
                    item.getLeExternalIdentifier());
            item.setLeDeletionFlag("N");
            deletionFlag = "N";
        } else if (!deletionFlag.equalsIgnoreCase("Y") && !deletionFlag.equalsIgnoreCase("N")) {
            log.warn("Data_Distributor_LE_Deletion Invalid deletion flag '{}' for LE_EXTERNAL_IDENTIFIER: {}, defaulting to 'N'",
                    deletionFlag, item.getLeExternalIdentifier());
            errorLogDAO.logValidationError(
                    RecordType.LEGAL_ENTITY_DELETION,
                    item.getLeExternalIdentifier(),
                    "Invalid LE_DELETIONFLAG value: " + deletionFlag + ". Must be 'Y' or 'N'. Defaulting to 'N'.",
                    null,
                    null,
                    fileId
            );
            item.setLeDeletionFlag("N");
            deletionFlag = "N";
        }

        // Lookup DD_UUID from mapping table (can be null if not found)
        String ddUuid = leUuidMap.get(item.getLeExternalIdentifier());
        item.setDdUuid(ddUuid);

        // Always add to records to insert - save ALL incoming data to DD_LEGALENTITY_DELETE_TBL
        recordsToInsert.add(item);
        log.info("Data_Distributor_LE_Deletion Record added for DB insert - LE_EXTERNAL_IDENTIFIER: {}, DD_UUID: {}, FLAG: {}",
                item.getLeExternalIdentifier(), ddUuid != null ? ddUuid : "NOT_FOUND", deletionFlag);

        // Only add to wrapper for API processing if deletion flag is 'Y' AND DD_UUID exists
        if ("Y".equalsIgnoreCase(item.getLeDeletionFlag())) {
            if (ddUuid != null) {
                taskletWrapper.getLeDeletionDomainWrapper().add(item);
                log.info("Data_Distributor_LE_Deletion Record added for API processing - LE_EXTERNAL_IDENTIFIER: {}, DD_UUID: {}",
                        item.getLeExternalIdentifier(), ddUuid);
            } else {
                log.warn("Data_Distributor_LE_Deletion LE External Identifier not found in mapping table, skipping API processing: {}",
                        item.getLeExternalIdentifier());
                errorLogDAO.logValidationError(
                        RecordType.LEGAL_ENTITY_DELETION,
                        item.getLeExternalIdentifier(),
                        "LE External Identifier not found in mapping table. Record saved but skipping API processing.",
                        null,
                        null,
                        fileId
                );
            }
        } else {
            log.info("Data_Distributor_LE_Deletion Record with flag 'N' saved but skipped for API - LE_EXTERNAL_IDENTIFIER: {}",
                    item.getLeExternalIdentifier());
        }

        return item;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info("Data_Distributor_LE_Deletion Preloading LE UUID mapping table into memory");
        leUuidMap = mappingDAO.fetchAllLEUuidMappings();
        this.fileId = stepExecution.getJobParameters().getString("leDeletionFileId");
        log.info("Data_Distributor_LE_Deletion Loaded {} UUIDs from mapping table", leUuidMap.size());
    }

    /**
     * Flush all valid records to DD_LEGALENTITY_DELETE_TBL
     */
    public void flushRecords() {
        if (!recordsToInsert.isEmpty()) {
            log.info("Data_Distributor_LE_Deletion Flushing {} deletion records to DB", recordsToInsert.size());
            deletionDAO.batchInsertDeleteRecords(recordsToInsert);
            recordsToInsert.clear();
        }
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        flushRecords();
        log.info("Data_Distributor_LE_Deletion Step completed. Records for API processing: {}",
                taskletWrapper.getLeDeletionDomainWrapper().size());
        return ExitStatus.COMPLETED;
    }
}

package com.ing.datadist.batch.processor;

import com.ing.datadist.dao.ErrorLogDAO;
import com.ing.datadist.dao.MappingDAO;
import com.ing.datadist.domain.LegalEntityChildDomain;
import com.ing.datadist.domain.TaskletCofaceChildDomainWrapper;
import com.ing.datadist.enums.RecordType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Slf4j
public class LegalEntityChildEnrichmentProcessor
        implements ItemProcessor<LegalEntityChildDomain, LegalEntityChildDomain>, StepExecutionListener {

    private final MappingDAO mappingDAO;
    private final TaskletCofaceChildDomainWrapper wrapper;
    private final ErrorLogDAO errorLogDAO;
    private String fileId;

    public LegalEntityChildEnrichmentProcessor(MappingDAO mappingDAO,
                                               TaskletCofaceChildDomainWrapper wrapper, ErrorLogDAO errorLogDAO) {
        this.mappingDAO = mappingDAO;
        this.wrapper = wrapper;
        this.errorLogDAO = errorLogDAO;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        this.fileId = stepExecution.getJobParameters().getString("leChildFileId");
    }

    @Override
    public LegalEntityChildDomain process(LegalEntityChildDomain item) {
        item.setFileId(this.fileId);

        String leExternalIdentifier = item.getLeExternalIdentifier();
        String parentUUID = mappingDAO.findParentUUIDByExternalIdentifier(leExternalIdentifier);
        if (parentUUID != null) {
            item.setOrgUUID(parentUUID);
            wrapper.getCofaceChildDomainWrapperMap()
                    .computeIfAbsent(leExternalIdentifier, k -> new ArrayList<>())
                    .add(item);
            return item;
        }

        errorLogDAO.logValidationError(
                RecordType.ORGANISATION_CHILD,
                leExternalIdentifier,
                "Parent record not found for LE External Identifier: " + leExternalIdentifier,
                item.getBatchId(),
                item.getFileName(),
                this.fileId
        );
        return null;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {

        long processed = wrapper.getCofaceChildDomainWrapperMap()
                .values().stream().mapToLong(List::size).sum();
        long failed = stepExecution.getProcessSkipCount();

        stepExecution.getJobExecution().getExecutionContext().put("totalRecords", processed + failed);
        stepExecution.getJobExecution().getExecutionContext().put("processedRecords", processed);
        stepExecution.getJobExecution().getExecutionContext().put("failedRecords", failed);

        return ExitStatus.COMPLETED;
    }
}package com.ing.datadist.batch.processor;

import com.ing.datadist.dao.MappingDAO;
import com.ing.datadist.domain.LegalEntityParentDomain;
import com.ing.datadist.domain.TaskletCofaceParentDomainWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class LEParentEnrichmentProcessor implements ItemProcessor<LegalEntityParentDomain, LegalEntityParentDomain>, StepExecutionListener {
    private final MappingDAO dao;
    private final TaskletCofaceParentDomainWrapper  taskletCofaceParentDomainWrapper;
    private final List<LegalEntityParentDomain> newLEMappings = new ArrayList<LegalEntityParentDomain>();
    private Map<String, String> leUuidMap;
    private String fileId;

    public LEParentEnrichmentProcessor(MappingDAO dao, TaskletCofaceParentDomainWrapper taskletCofaceParentDomainWrapper) {
        this.dao = dao;
        this.taskletCofaceParentDomainWrapper = taskletCofaceParentDomainWrapper;

    }

    @Override
    public LegalEntityParentDomain process(LegalEntityParentDomain item) throws Exception {
       item.setFileId(this.fileId);
        String orgUUID = leUuidMap.get(item.getLeExternalIdentifier());
        item.setFileId(this.fileId);
        if (orgUUID == null) {
            orgUUID = UUID.randomUUID().toString();
            item.setOrgUUID(orgUUID);
            newLEMappings.add(item);
            leUuidMap.put(item.getLeExternalIdentifier(), orgUUID);
        }
        item.setOrgUUID(orgUUID);
        taskletCofaceParentDomainWrapper.getCofaceParentDomainWrapper().add(item);
        return item;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info(" Preloading mapping table into memory");
        leUuidMap = dao.fetchAllLEUuidMappings();
        this.fileId = stepExecution.getJobParameters().getString("leParentFileId");
        log.info("Loaded {} UUIDs from mapping table", leUuidMap.size());
    }

    public void flushNewMappings() {
        if (!newLEMappings.isEmpty()) {
            log.info("Flushing {} new LE mapping records to DB", newLEMappings.size());
            dao.batchInsertLEMappingRecords(newLEMappings);
            newLEMappings.clear();
        }
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {

        long processed = taskletCofaceParentDomainWrapper.getCofaceParentDomainWrapper().size();
        long failed = stepExecution.getProcessSkipCount();

        stepExecution.getJobExecution().getExecutionContext().put("totalRecords", processed + failed);
        stepExecution.getJobExecution().getExecutionContext().put("processedRecords", processed);
        stepExecution.getJobExecution().getExecutionContext().put("failedRecords", failed);

        flushNewMappings();

        return ExitStatus.COMPLETED;
    }
}
package com.ing.datadist.batch.processor;

import com.ing.datadist.dao.MappingDAO;
import com.ing.datadist.domain.OrganisationUnitDomain;
import com.ing.datadist.domain.OrganisationUnitDomainWrapper;
import com.ing.datadist.domain.TaskletCofaceOpsDomainWrapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;

import java.util.*;

@Slf4j
@Getter
@StepScope
public class OrganisationUnitEnrichmentProcessor implements ItemProcessor<OrganisationUnitDomain, OrganisationUnitDomainWrapper>, StepExecutionListener {
    private final MappingDAO dao;
    private final TaskletCofaceOpsDomainWrapper taskletCofaceOpsDomainWrapper;
    private final List<OrganisationUnitDomainWrapper> newOPsMappings = new ArrayList<OrganisationUnitDomainWrapper>();
    private final List<OrganisationUnitDomainWrapper> newLEMappings = new ArrayList<OrganisationUnitDomainWrapper>();
    private Map<String, String> opsUuidMap;
    private String fileId;

    public OrganisationUnitEnrichmentProcessor(MappingDAO dao, TaskletCofaceOpsDomainWrapper taskletCofaceOpsDomainWrapper) {
        this.dao = dao;
        this.taskletCofaceOpsDomainWrapper = taskletCofaceOpsDomainWrapper;
    }

    @Override
    public OrganisationUnitDomainWrapper process(OrganisationUnitDomain item) throws Exception {
        String fileId = item.getFileId();
        String orgUUID = opsUuidMap.get(item.getOrgExternalIdentifierVal());
        if (orgUUID != null) {
            String ipExternalIdentifierVal = item.getOpsExternalIdentifierVal();
            String opsUUID = opsUuidMap.get(ipExternalIdentifierVal);
            OrganisationUnitDomainWrapper record = new OrganisationUnitDomainWrapper();
            record.setFileId(this.fileId); // Set fileId here
            if (opsUUID == null) {
                log.info("No UUID for CofaceOpsId {}. Adding to batch insert.", ipExternalIdentifierVal);
                opsUUID = UUID.randomUUID().toString();
                log.info("Data_Distributor_Coface_OU No UUID for CofaceOpsId {}, Creating uuid:{}", ipExternalIdentifierVal,opsUUID);
                record.setOpsUUID(opsUUID); // Set the new UUID
                newOPsMappings.add(record);
                opsUuidMap.put(ipExternalIdentifierVal, opsUUID);
            }
            record.setOrgUUID(orgUUID);
            record.setOpsUUID(opsUUID);
            record.setOpsExternalIdentifierVal(item.getOpsExternalIdentifierVal());
            record.setOrganisationUnitName(item.getOrganisationUnitName());
            record.setAddress(item.getAddress());
            record.setOrgExternalIdentifierVal(item.getOrgExternalIdentifierVal());
            record.setDigitalAddrEmail(item.getDigitalAddrEmail());
            record.setDigitalAddrFullTel(item.getDigitalAddrFullTel());
            record.setDigitalAddrFullTefgn(item.getDigitalAddrfFullTefgn());
            record.setPostalAddressCityName(item.getPostalAddressCityName());
            record.setPostalAddressCntryCd(item.getPostalAddressCntryCd());
            record.setPostalAddressHouseAdd(item.getPostalAddressHouseAdd());
            record.setPostalAddressHouseNum(item.getPostalAddressHouseNum());
            record.setPostalAddressPostalCd(item.getPostalAddressPostalCd());
            record.setPostalAddressStreetNm(item.getPostalAddressStreetNm());
            record.setOpsExtIdDateOfIssue(item.getOpsExtIdDateOfIssue());
            record.setOpsExtIdExpiryDate(item.getOpsExtIdExpiryDate());
            taskletCofaceOpsDomainWrapper.getCofaceOpsDomainWrapper().add(record);
            return record;
        } else {
            log.error("Data_Distributor_Coface_OU UUID for Organisation  id:{} of OperationSite id:{} not available ", item.getOrgExternalIdentifierVal(), item.getOpsExternalIdentifierVal());
            return null;
        }
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info(" Preloading mapping table into memory");
        opsUuidMap = dao.fetchAllOPsUuidMappings();
        opsUuidMap.putAll(dao.fetchAllLEUuidMappings());

        // Extract fileId from job parameters
        this.fileId = stepExecution.getJobParameters().getString("opsFileId");
        log.info("Loaded {} UUIDs from mapping table", opsUuidMap.size());
    }


    public void flushNewMappings() {
        if (!newOPsMappings.isEmpty()) {
            log.info("Flushing {} new OPS mapping records to DB", newOPsMappings.size());
            dao.batchInsertOPsMappingRecords(newOPsMappings);
            newOPsMappings.clear();
        }
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {

        long processed = taskletCofaceOpsDomainWrapper.getCofaceOpsDomainWrapper().size();
        long failed = stepExecution.getProcessSkipCount();

        stepExecution.getJobExecution().getExecutionContext().put("totalRecords", processed + failed);
        stepExecution.getJobExecution().getExecutionContext().put("processedRecords", processed);
        stepExecution.getJobExecution().getExecutionContext().put("failedRecords", failed);

        flushNewMappings();

        return ExitStatus.COMPLETED;
    }
}
package com.ing.datadist.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Domain class for File Ingestion tracking
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FileIngestionDomain {

    private String fileId;
    private String batchId;
    private String fileName;
    private String filePath;
    private Long fileSize;
    private String fileChecksum;
    private String flowName;
    private String flowType;
    private String frequency;
    private String sourceSystem;
    private String status;
    private LocalDateTime receivedTs;
    private LocalDateTime processingStartTs;
    private LocalDateTime processingEndTs;
    private Long totalRecords;
    private Long processedRecords;
    private Long failedRecords;
    private Long skippedRecords;
    private String errorMessage;
    private Integer retryCount;
    private String createdBy;
    private LocalDateTime createdTs;
    private String updatedBy;
    private LocalDateTime updatedTs;
}
package com.ing.datadist.domain;

import lombok.Data;

@Data
public class LegalEntityChildDomain {
    private String orgUUID;
    private String leExternalIdentifier;
    private String industryClassCode;
    private Integer industryClassRank;
    private String industryClassEffDate;
    private String industryClassEndDate;
    private String batchId;
    private String fileName;
    private String fileId;
}

package com.ing.datadist.domain;

import lombok.Data;

/**
 * Domain class for Legal Entity Deletion records from COFACE_LE_DEL.csv
 */
@Data
public class LegalEntityDeletionDomain {
    private Long id;
    private String leExternalIdentifier;
    private String leDeletionFlag;
    private String ddUuid;
    private String fileId;
}

package com.ing.datadist.domain;

import lombok.Data;

@Data
public class LegalEntityParentDomain {
    private String leExternalIdentifier;
    private String orgStatus;
    private String orgName;
    private String orgOtherName;
    private String orgOtherNameType;
    private String adrUnstructuredAddress;
    private String adrPostalAddressStreetName;
    private String adrPostalAddressHouseNum;
    private String adrPostalAddressHouseNumAdd;
    private String adrPostalAddressPostalCode;
    private String adrPostalAddressCityName;
    private String adrPostalAddressCountryCode;
    private String orgLegalForm;
    private String orgBusinessClosedownDate;
    private String lELegalStatus;
    private String asmtIdVerifyApprovalDate;
    private String lEPreferredLanguage;
    private String adrDigitalAddrFullTel;
    private String nssoExternalIdentifier;
    private String asmtVatStatus;
    private String nssoExternalIdentifierStatus;
    private String orgUUID; // Add UUID field for mapping
    private String batchId;
    private String fileName;
    private String fileId;

}
package com.ing.datadist.domain;

import lombok.Data;

@Data
public class OrganisationUnitDomain {
    private String orgExternalIdentifierVal;
    private String opsExternalIdentifierVal;
    private String organisationUnitName;
    private String address;
    private String postalAddressStreetNm;
    private String postalAddressHouseNum;
    private String postalAddressHouseAdd;
    private String postalAddressPostalCd;
    private String postalAddressCityName;
    private String postalAddressCntryCd;
    private String opsExtIdDateOfIssue;
    private String opsExtIdExpiryDate;
    private String digitalAddrFullTel;
    private String digitalAddrfFullTefgn;
    private String digitalAddrEmail;
    private String fileId;
}
package com.ing.datadist.domain;

import lombok.Data;

@Data
public class OrganisationUnitDomainWrapper {
    private String opsUUID;
    private String orgUUID;
    private String orgExternalIdentifierVal;
    private String opsExternalIdentifierVal;
    private String organisationUnitName;
    private String address;
    private String postalAddressStreetNm;
    private String postalAddressHouseNum;
    private String postalAddressHouseAdd;
    private String postalAddressPostalCd;
    private String postalAddressCityName;
    private String postalAddressCntryCd;
    private String opsExtIdDateOfIssue;
    private String opsExtIdExpiryDate;
    private String digitalAddrFullTel;
    private String digitalAddrFullTefgn;
    private String digitalAddrEmail;
    private String onePamUuid;
    private String fileId;
}
package com.ing.datadist.domain;

import lombok.Data;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@JobScope
@Data
public class TaskletCofaceParentDomainWrapper {

    private List<LegalEntityParentDomain> cofaceParentDomainWrapper = new ArrayList<LegalEntityParentDomain>();

}
package com.ing.datadist.domain;

import lombok.Data;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@JobScope
@Data
public class TaskletCofaceOpsDomainWrapper {

    private List<OrganisationUnitDomainWrapper> cofaceOpsDomainWrapper = new ArrayList<OrganisationUnitDomainWrapper>();
}
package com.ing.datadist.domain;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Component
@JobScope
@Data
public class TaskletCofaceChildDomainWrapper{

    private HashMap<String,List<LegalEntityChildDomain>> cofaceChildDomainWrapperMap;

    @PostConstruct
    public void init(){
        this.cofaceChildDomainWrapperMap=new HashMap<>();
    }

}package com.ing.datadist.domain;

import lombok.Data;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper class for Legal Entity Deletion batch processing
 */
@Component
@JobScope
@Data
public class TaskletLeDeletionDomainWrapper {

    private List<LegalEntityDeletionDomain> leDeletionDomainWrapper = new ArrayList<>();

    public void clear() {
        leDeletionDomainWrapper.clear();
    }
}

#=======================================================================================================================
# Spring Configuration
#=======================================================================================================================
app:
  input-file: classpath:cofaceOpsData.csv
  le-parent-file: classpath:cofaceLEParent.csv
  le-child-file: classpath:CofaceLEChild.csv
  incapables-file: classpath:CofaceIncapables.csv
  le-delete-file: classpath:COFACE_LE_DEL.csv
  apiFlow: true
  initialLoad: false
ecs:
  access-key: default
  secret-key: default
  region: default
  url: default
  bucketName: default
  uploadBucketName: default
  cofaceOUFileName: cofaceOpsData.csv
  cofaceLEParentFileName: cofaceLEParent.csv
  cofaceLEChildFileName: CofaceLEChild.csv
  cofaceIncapablesFileName: CofaceIncapables.csv
  readFileFrom: RESOURCES
  output-file: OilFiles
  s3:
    enabled: false


spring:
  application:
    name: Integration-Module
  autoconfigure:
    exclude: org.springframework.boot.autoconfigure.ssl.SslAutoConfiguration,
     org.springframework.boot.actuate.autoconfigure.ssl.SslHealthContributorAutoConfiguration
  sql:
    init:
      mode: embedded
      schema-locations: classpath:org/springframework/batch/core/schema-oracle.sql
  datasource:
    url: jdbc:oracle:thin:@//localhost:1521/XE
    username: system
    password: 1616aastha
    driver-class-name: oracle.jdbc.OracleDriver
  batch:
    schedular:
      legalEntityCron: "0 */10 * * * *"
      organisationUnitCron: "0 */1 * * * *"
      incapablesCron: "0 */50 * * * *"
      leDeletionCron: "0 */50 * * * *"
      notificationStatusCron: "0 0 0 * * *"
      notificationFailStatusCron: "0 0 0 * * *"
      notificationTimeOutStatusCron: "0 0 0 * * *"
    jdbc:
      initialize-schema: always
    initialize-schema: always
    input-file: #input-file
    job:
      enabled: false
      name: organisationUnitEnrichmentJob
      initialize-schema: never
  kafka:
    consumer:
      bootstrap-servers-config: "br201-odin-tst.ic.ing.net:9093,br401-odin-tst.ic.ing.net:9093"

      avro:
        shared-secret: "73FF6232475EFB3370AF8E5EE2E574704D62823E2F8B6848A198B386BFAC36AD"
        schema:
          registry:
            url: "https://sr1-global-tst.ing.net:8443"
    producer:
      avroSchemaRegistryUrls: https://sr1-global-tst.ing.net:8443,https://sr2-global-tst.ing.net:8443
      odinBootstrapServers: br101-odin-tst.ic.ing.net:9093,br201-odin-tst.ic.ing.net:9093,br301-odin-tst.ic.ing.net:9093,br401-odin-tst.ic.ing.net:9093
      ssl-enabled: true
      topic: P03102.SendS3ObjectAsFile-GLO0-PartyDataIngest
      replyTopic: P33558.DataDistributorkafkaECStest.topic
      clientId: PartyDataIngest
      flowId: FLT23555
      partnerId: ONEPAM_IL-T_SSL
      replyLevel: FULL
      replyCluster: odin
    common:
      auto-offset-rest: "earliest"
    ssl:
      enabled: true
      key-store-location: "classpath:identity.jks"
      keystore-type: JKS
      key-store-password: publicpass
      key-password: publicpass
      trust-store-location: "classpath:http-trust.jks"
      trust-store-password: publicpass
      truststore-type: JKS
      protocol: TLSv1.2
    security:
      protocol: SSL
kafka:
  consumers:
    group-id: "onepam_test_group_local-005-xyz"
    organisation-unit:
      topic-name: "P00007.ingbeextnotifyorganisationunit5topic"
    organisation:
      topic-name: "P00007.ingbeextnotifyorganisation5topic"
    digital-address:
      topic-name: "P00007.ingbeextnotifydigitaladdress5topic"
    postal-address:
      topic-name: "P00007.ingbeextnotifypostaladdress5topic"
    organisation-hierarchy:
      topic-name: "P00007.ingbeextnotifyorganisationhierarchy5topic"
    industry-classification:
      topic-name: "P00007.ingbeextnotifyindustryclassification5topic"
    organisation-unit-name:
      topic-name: "P00007.ingbeextnotifyorganisationunitname5topic"
    internal-identifier:
      topic-name: "P00007.ingbeextnotifyinvolvedpartyinternalidentifier5topic"
    external-identifier:
      topic-name: "P00007.ingbeextnotifyinvolvedpartyexternalidentifier5topic"
    organisation-name:
      topic-name: "P00007.ingbeextnotifyorganisationname5topic"
    assessment:
      topic-name: "P00007.ingbeextnotifyassessment5topic"
    incapable-individual:
      topic-name: "P00007.ingbeextnotifyindividual5topic"
    incapable-individual-name:
      topic-name: "P00007.ingbeextnotifyindividualname5topic"
    incapable-occupation:
      topic-name: "P00007.ingbeextnotifyoccupation5topic"
    incapable-marking:
      topic-name: "P00007.ingbeextnotifyinvolvedpartymarking5topic"


merak:
  filter:
    peer-token-filter:
      enabled: false
    access-token-filter:
      enabled: false
  administration:

    metrics:
      enabled: true
      kafka-enabled: true
      jvm-metrics-enabled: true
      publish-enabled: true
    server:
      bind-host: localhost
      bind-port: 10080
  insights:
    kafka:
      enabled: false
      tls-enabled: false
  logging:
    kafka:
      enabled: false
      tls-enabled: false
  tracing:
    reporting:
      kafka:
        enabled: false
        tls-enabled: false

# =======================================================================================================================
# Server Configuration
# =======================================================================================================================
server:
  servlet.context-path: /
  port: 8181
  tomcat:
    basedir: ./
    logdir: ./
    accesslog:
      enabled: false
  ssl:
    enabled: false
    key-store: classpath:keystore.jks
    key-store-password: publicpass
    trust-store: classpath:http-trust.jks
    trust-store-password: publicpass
    key-alias: certificate
    key-password: publicpass
    key-store-type: PKCS12
    trust-store-type: JKS
    client-auth: need
    enabled-protocols: TLSv1.2
    ciphers: TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256

api-trust-tokens:
  access-tokens:
    keystore:
      location: classpath:accesstokens.jks
      password: publicpass
  peer-tokens:
    keystore:
      location: classpath:peertokens.jks
      password: publicpass
  manifest:
    trust-store:
      location: classpath:manifest.jks
      password: publicpass
    location: classpath:manifest.jose
    signed: false


# =======================================================================================================================
# OnePam API Configuration
# =======================================================================================================================
involved-party:
  default:
    debugEnabled: true
    sessionTimeout: 500
    timeout: 3000
  endpoints:
    CREATE_INVOLVED_PARTY:
      url: "https://apis.ing.com/v5/involved-parties"
      method: "POST"
    UPDATE_INVOLVED_PARTY:
      url: "/v5/involved-parties/{uuid}"
      method: "PATCH"
    UPDATE_EXTERNAL_IDENTIFIER:
      url: "https://apis.ing.com/v5/involved-parties/{uuid}/external-identifiers"
      method: "POST"
    CREATE_ORGANISATION_UNIT_HIERARCHY:
      url: "v1/organisation-hierarchies/relationship"
      method: "POST"
    SEARCH_ORGANISATION_UNIT:
      url: "/v2/partyandagreementsearch/involved-parties/organisation-units/search"
      method: "POST"

# =======================================================================================================================
# Logging Configuration
# =======================================================================================================================

logging:
  level:
    root: INFO
    com.ing: DEBUG
    org.apache.kafka.clients: INFO

service:
  instance:
    operational-mode:
      initial-mode: "live"
      default-mode: "live"
    datacenter: WPR
#=======================================================================================================================
#Peer Connection configuration
#=======================================================================================================================
http-client:
  ssl:
    enabled-protocols: TLSv1.2
    ciphers: TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
  peer-token:
    enabled: true
  routing-client:
    defaults:
      global-request-timeout: 10s
      individual-request-timeout: 10s
      debugEnabled: true

#=======================================================================================================================
# Data Retention Configuration
#=======================================================================================================================
data-retention:
  enabled: true
  retention-days: 10
  cleanup-cron: "0 */50 * * * *"
  batch-size: 100
  delete-s3-files: true

  parent-table:
    name: DD_FILE_INGESTION_TBL
    file-id-field: FILE_ID
    timestamp-field: PROCESSING_END_TS
    description: "File ingestion tracking - master table"

  child-tables:
    # Delete child tables with FK constraints first, then tables without FK constraints
    - name: DD_MAP_TBL
      file-id-field: FILE_ID
      enabled: true
      description: "UUID mapping table"

    - name: DD_ERROR_LOG
      file-id-field: FILE_ID
      enabled: true
      description: "Error logging table"

    - name: DD_ORG_UNIT_ACCT_TBL
      file-id-field: FILE_ID
      enabled: true
      description: "Organisation Unit account mapping"

    - name: DD_ORG_PARENT_ACCT_TBL
      file-id-field: FILE_ID
      enabled: true
      description: "Organisation parent account mapping"

    - name: DD_ORG_CHILD_ACCT_TBL
      file-id-field: FILE_ID
      enabled: true
      description: "Organisation child account mapping"

    - name: DD_COFACEOPS_TBL
      file-id-field: FILE_ID
      enabled: true
      description: "Coface Organisation Unit intermediate data"

    - name: DD_LEGAL_ENTITY_PARENT_TBL
      file-id-field: FILE_ID
      enabled: true
      description: "Legal Entity parent data from Coface"

    - name: DD_LEGAL_ENTITY_CHILD_TBL
      file-id-field: FILE_ID
      enabled: true
      description: "Legal Entity child data (industry classifications)"

    - name: DD_EVENT_TRACK_TBL
      file-id-field: FILE_ID
      enabled: true
      description: "Event tracking for Kafka notifications"

    - name: DD_NOTIFICATION_STATUS
      file-id-field: FILE_ID
      enabled: true
      description: "Final notification status records"

    - name: DD_LEGALENTITY_DELETE_TBL
      file-id-field: FILE_ID
      enabled: true
      description: "LE Deletion Records"
