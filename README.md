diff --git a/DataDistribution/src/main/java/com/ing/datadist/DataDistributionApplication.java b/DataDistribution/src/main/java/com/ing/datadist/DataDistributionApplication.java
index f1cca3f..0f2ff46 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/DataDistributionApplication.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/DataDistributionApplication.java
@@ -8,6 +8,7 @@ import org.springframework.scheduling.annotation.EnableScheduling;
 
 @EnableScheduling
 @SpringBootApplication
+
 public class DataDistributionApplication {
     public static void main(String[] args) {
         SpringApplication.run(DataDistributionApplication.class, args);
diff --git a/DataDistribution/src/main/java/com/ing/datadist/batch/config/BatchConstants.java b/DataDistribution/src/main/java/com/ing/datadist/batch/config/BatchConstants.java
index 3db0176..3118218 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/batch/config/BatchConstants.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/batch/config/BatchConstants.java
@@ -23,12 +23,6 @@ public final class BatchConstants {
     public static final String INCAPABLES_BATCH_STEP_NAME = "incapablesEnrichmentStep";
     public static final String INCAPABLES_BATCH_JOB_NAME = "IncapablesEnrichmentJob";
 
-    // Legal Entity Deletion Batch Constants
-    public static final String COFACE_LE_DELETION_BATCH_JOB_NAME = "LegalEntityDeletionJob";
-    public static final String COFACE_LE_DELETION_BATCH_FILE_READER_NAME = "legalEntityDeletionReader";
-    public static final String COFACE_LE_DELETION_BATCH_STEP_NAME = "legalEntityDeletionEnrichStep";
-    public static final String LE_DELETION_API_TASKLET_STEP = "leDeletionApiTaskletStep";
-
     // Tasklet Step Names
     public static final String ORG_UNIT_SEARCH_API_TASKLET_STEP = "organisationUnitSearchApiTaskletStep";
     public static final String ORG_SEARCH_API_TASKLET_STEP = "organisationSearchApiTaskletStep";
diff --git a/DataDistribution/src/main/java/com/ing/datadist/batch/config/BatchJobConfig.java b/DataDistribution/src/main/java/com/ing/datadist/batch/config/BatchJobConfig.java
index 5bac3a2..71449bf 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/batch/config/BatchJobConfig.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/batch/config/BatchJobConfig.java
@@ -2,13 +2,12 @@ package com.ing.datadist.batch.config;
 
 import com.ing.datadist.batch.processor.*;
 import com.ing.datadist.batch.processor.OrganisationUnitEnrichmentProcessor;
-import com.ing.datadist.batch.tasklet.LEDeletionApiTasklet;
 import com.ing.datadist.batch.tasklet.OrganisationSearchApiTasklet;
 import com.ing.datadist.batch.tasklet.OrganisationUnitSearchApiTasklet;
-import com.ing.datadist.dao.LegalEntityDeletionDAO;
 import com.ing.datadist.dao.MappingDAO;
 import com.ing.datadist.dao.queries.IncapablesQueries;
 import com.ing.datadist.dao.ErrorLogDAO;
+import com.ing.datadist.dao.MappingDAO;
 import com.ing.datadist.domain.*;
 import com.ing.datadist.domain.fileinput.LegalEntityParent;
 import com.ing.datadist.domain.fileinput.OrganisationUnit;
@@ -125,10 +124,6 @@ public class BatchJobConfig {
     @Autowired
     private TaskletCofaceChildDomainWrapper taskletCofaceChildDomainWrapper;
     @Autowired
-    private TaskletLeDeletionDomainWrapper taskletLeDeletionDomainWrapper;
-    @Autowired
-    private LegalEntityDeletionDAO legalEntityDeletionDAO;
-    @Autowired
     private FileIngestionService fileIngestionService;
     @Value("${ecs.bucketName}")
     private String bucketName;
@@ -140,8 +135,6 @@ public class BatchJobConfig {
     private String cofaceLEChildFileName;
     @Value("${ecs.cofaceIncapablesFileName}")
     private String cofaceIncapablesFileName;
-    @Value("${ecs.cofaceLEDeleteFileName:COFACE_LE_DEL.csv}")
-    private String cofaceLEDeleteFileName;
     @Value("${ecs.readFileFrom}")
     private String readFileFrom;
     @Value("${app.input-file}")
@@ -152,8 +145,6 @@ public class BatchJobConfig {
     private Resource resourceLEChildFile;
     @Value("${app.incapables-file}")
     private Resource resourceIncapablesFile;
-    @Value("${app.le-delete-file:classpath:COFACE_LE_DEL.csv}")
-    private Resource resourceLEDeleteFile;
 
     @Bean
     @StepScope
@@ -475,89 +466,6 @@ public class BatchJobConfig {
                 .build();
     }
 
-    // Legal Entity Deletion batch config start ----------------------------------------------------------------------------------------
-
-    @Bean
-    @StepScope
-    public FlatFileItemReader<LegalEntityDeletionDomain> legalEntityDeletionReader() {
-        String fileId = StepSynchronizationManager.getContext().getStepExecution()
-                .getJobParameters().getString("leDeletionFileId");
-        return new FlatFileItemReaderBuilder<LegalEntityDeletionDomain>()
-                .name(COFACE_LE_DELETION_BATCH_FILE_READER_NAME)
-                .resource(getFileForLocalRun(readFileFrom, cofaceLEDeleteFileName, fileId))
-                .delimited()
-                .names(LegalEntityDeletionFieldSetMapper.LE_EXTERNAL_IDENTIFIER,
-                       LegalEntityDeletionFieldSetMapper.LE_DELETION_FLAG)
-                .fieldSetMapper(new LegalEntityDeletionFieldSetMapper())
-                .linesToSkip(1)
-                .build();
-    }
-
-    @Bean
-    public LEDeletionEnrichmentProcessor leDeletionEnrichmentProcessor() {
-        return new LEDeletionEnrichmentProcessor(mappingDAO, legalEntityDeletionDAO, taskletLeDeletionDomainWrapper, errorLogDAO);
-    }
-
-    @Bean
-    public JdbcBatchItemWriter<LegalEntityDeletionDomain> legalEntityDeletionWriter(DataSource dataSource) {
-        String sql = "INSERT INTO DD_LEGALENTITY_DELETE_TBL (LE_DELETION_FLAG, DD_UUID, FILE_ID, CREATED_BY, CREATED_TS) " +
-                     "VALUES (:leDeletionFlag, :ddUuid, :fileId, 'DD_USER', SYSTIMESTAMP)";
-        return new JdbcBatchItemWriterBuilder<LegalEntityDeletionDomain>()
-                .sql(sql)
-                .beanMapped()
-                .dataSource(dataSource)
-                .build();
-    }
-
-    @Bean
-    public Step legalEntityDeletionEnrichStep(JobRepository jobRepository,
-                                               PlatformTransactionManager transactionManager,
-                                               FlatFileItemReader<LegalEntityDeletionDomain> legalEntityDeletionReader,
-                                               LEDeletionEnrichmentProcessor leDeletionEnrichmentProcessor,
-                                               JdbcBatchItemWriter<LegalEntityDeletionDomain> legalEntityDeletionWriter) {
-        return new StepBuilder(COFACE_LE_DELETION_BATCH_STEP_NAME, jobRepository)
-                .<LegalEntityDeletionDomain, LegalEntityDeletionDomain>chunk(100, transactionManager)
-                .reader(legalEntityDeletionReader())
-                .processor(leDeletionEnrichmentProcessor)
-                .writer(legalEntityDeletionWriter)
-                .listener((StepExecutionListener) leDeletionEnrichmentProcessor)
-                .listener(new ChunkListener() {
-                    @Override
-                    public void afterChunk(ChunkContext context) {
-                        leDeletionEnrichmentProcessor.flushRecords();
-                    }
-                })
-                .faultTolerant()
-                .skip(FlatFileParseException.class)
-                .skipLimit(100)
-                .retryLimit(3)
-                .retry(Exception.class)
-                .noRetry(FlatFileParseException.class)
-                .build();
-    }
-
-    @Bean
-    public Step leDeletionApiTaskletStep(JobRepository jobRepository,
-                                          PlatformTransactionManager transactionManager,
-                                          LEDeletionApiTasklet leDeletionApiTasklet) {
-        return new StepBuilder(LE_DELETION_API_TASKLET_STEP, jobRepository)
-                .tasklet(leDeletionApiTasklet, transactionManager)
-                .build();
-    }
-
-    @Bean(name = "LegalEntityDeletionJob")
-    public Job legalEntityDeletionJob(JobRepository jobRepository,
-                                       Step legalEntityDeletionEnrichStep,
-                                       Step leDeletionApiTaskletStep) {
-        log.info("LegalEntityDeletionJob job started");
-        return new JobBuilder(COFACE_LE_DELETION_BATCH_JOB_NAME, jobRepository)
-                .incrementer(new RunIdIncrementer())
-                .start(legalEntityDeletionEnrichStep)
-                .next(leDeletionApiTaskletStep)
-                .build();
-    }
-    // Legal Entity Deletion batch config End ----------------------------------------------------------------------------------------
-
 
     // Read File from ECS/Local-folder --------------------------------------------------------------------------------------
     private Resource getFileForLocalRun(String fileFrom, String fileName, String fileId) {
@@ -573,8 +481,6 @@ public class BatchJobConfig {
             resource = resourceOUFile;
         } else if (fileName.equalsIgnoreCase(cofaceIncapablesFileName)) {
             resource = resourceIncapablesFile;
-        } else if (fileName.equalsIgnoreCase(cofaceLEDeleteFileName)) {
-            resource = resourceLEDeleteFile;
         } else {
             log.error("DD_Data_Distributor Unknown File Name {}", fileName);
             return null;
diff --git a/DataDistribution/src/main/java/com/ing/datadist/batch/config/LegalEntityDeletionFieldSetMapper.java b/DataDistribution/src/main/java/com/ing/datadist/batch/config/LegalEntityDeletionFieldSetMapper.java
deleted file mode 100644
index 00ef8e9..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/batch/config/LegalEntityDeletionFieldSetMapper.java
+++ /dev/null
@@ -1,23 +0,0 @@
-package com.ing.datadist.batch.config;
-
-import com.ing.datadist.domain.LegalEntityDeletionDomain;
-import org.springframework.batch.item.file.mapping.FieldSetMapper;
-import org.springframework.batch.item.file.transform.FieldSet;
-import org.springframework.lang.NonNull;
-
-/**
- * FieldSetMapper for Legal Entity Deletion CSV file
- */
-public class LegalEntityDeletionFieldSetMapper implements FieldSetMapper<LegalEntityDeletionDomain> {
-
-    public static final String LE_EXTERNAL_IDENTIFIER = "LE_ExternalIdentifier";
-    public static final String LE_DELETION_FLAG = "LE_DELETIONFLAG";
-
-    @Override
-    public @NonNull LegalEntityDeletionDomain mapFieldSet(@NonNull FieldSet fieldSet) {
-        LegalEntityDeletionDomain domain = new LegalEntityDeletionDomain();
-        domain.setLeExternalIdentifier(fieldSet.readString(LE_EXTERNAL_IDENTIFIER));
-        domain.setLeDeletionFlag(fieldSet.readString(LE_DELETION_FLAG));
-        return domain;
-    }
-}
diff --git a/DataDistribution/src/main/java/com/ing/datadist/batch/processor/LEDeletionEnrichmentProcessor.java b/DataDistribution/src/main/java/com/ing/datadist/batch/processor/LEDeletionEnrichmentProcessor.java
deleted file mode 100644
index 0fe4da8..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/batch/processor/LEDeletionEnrichmentProcessor.java
+++ /dev/null
@@ -1,139 +0,0 @@
-package com.ing.datadist.batch.processor;
-
-import com.ing.datadist.dao.ErrorLogDAO;
-import com.ing.datadist.dao.LegalEntityDeletionDAO;
-import com.ing.datadist.dao.MappingDAO;
-import com.ing.datadist.domain.LegalEntityDeletionDomain;
-import com.ing.datadist.domain.TaskletLeDeletionDomainWrapper;
-import com.ing.datadist.enums.RecordType;
-import lombok.extern.slf4j.Slf4j;
-import org.springframework.batch.core.ExitStatus;
-import org.springframework.batch.core.StepExecution;
-import org.springframework.batch.core.StepExecutionListener;
-import org.springframework.batch.item.ItemProcessor;
-
-import java.util.ArrayList;
-import java.util.List;
-import java.util.Map;
-
-/**
- * Batch processor for Legal Entity Deletion flow
- * Similar to LEParentEnrichmentProcessor
- */
-@Slf4j
-public class LEDeletionEnrichmentProcessor implements ItemProcessor<LegalEntityDeletionDomain, LegalEntityDeletionDomain>, StepExecutionListener {
-
-    private final MappingDAO mappingDAO;
-    private final LegalEntityDeletionDAO deletionDAO;
-    private final TaskletLeDeletionDomainWrapper taskletWrapper;
-    private final ErrorLogDAO errorLogDAO;
-
-    private Map<String, String> leUuidMap;
-    private String fileId;
-    private final List<LegalEntityDeletionDomain> recordsToInsert = new ArrayList<>();
-
-    public LEDeletionEnrichmentProcessor(MappingDAO mappingDAO,
-                                         LegalEntityDeletionDAO deletionDAO,
-                                         TaskletLeDeletionDomainWrapper taskletWrapper,
-                                         ErrorLogDAO errorLogDAO) {
-        this.mappingDAO = mappingDAO;
-        this.deletionDAO = deletionDAO;
-        this.taskletWrapper = taskletWrapper;
-        this.errorLogDAO = errorLogDAO;
-    }
-
-    @Override
-    public LegalEntityDeletionDomain process(LegalEntityDeletionDomain item) throws Exception {
-        item.setFileId(this.fileId);
-
-        // Validate LE External Identifier
-        if (item.getLeExternalIdentifier() == null || item.getLeExternalIdentifier().trim().isEmpty()) {
-            log.warn("Data_Distributor_LE_Deletion Skipping record with empty LE External Identifier");
-            return null;
-        }
-
-        // Validate deletion flag - if null or invalid, default to 'N' and still save
-        String deletionFlag = item.getLeDeletionFlag();
-        if (deletionFlag == null || deletionFlag.trim().isEmpty()) {
-            log.warn("Data_Distributor_LE_Deletion Record with empty deletion flag, defaulting to 'N' for LE_EXTERNAL_IDENTIFIER: {}",
-                    item.getLeExternalIdentifier());
-            item.setLeDeletionFlag("N");
-            deletionFlag = "N";
-        } else if (!deletionFlag.equalsIgnoreCase("Y") && !deletionFlag.equalsIgnoreCase("N")) {
-            log.warn("Data_Distributor_LE_Deletion Invalid deletion flag '{}' for LE_EXTERNAL_IDENTIFIER: {}, defaulting to 'N'",
-                    deletionFlag, item.getLeExternalIdentifier());
-            errorLogDAO.logValidationError(
-                    RecordType.LEGAL_ENTITY_DELETION,
-                    item.getLeExternalIdentifier(),
-                    "Invalid LE_DELETIONFLAG value: " + deletionFlag + ". Must be 'Y' or 'N'. Defaulting to 'N'.",
-                    null,
-                    null,
-                    fileId
-            );
-            item.setLeDeletionFlag("N");
-            deletionFlag = "N";
-        }
-
-        // Lookup DD_UUID from mapping table (can be null if not found)
-        String ddUuid = leUuidMap.get(item.getLeExternalIdentifier());
-        item.setDdUuid(ddUuid);
-
-        // Always add to records to insert - save ALL incoming data to DD_LEGALENTITY_DELETE_TBL
-        recordsToInsert.add(item);
-        log.info("Data_Distributor_LE_Deletion Record added for DB insert - LE_EXTERNAL_IDENTIFIER: {}, DD_UUID: {}, FLAG: {}",
-                item.getLeExternalIdentifier(), ddUuid != null ? ddUuid : "NOT_FOUND", deletionFlag);
-
-        // Only add to wrapper for API processing if deletion flag is 'Y' AND DD_UUID exists
-        if ("Y".equalsIgnoreCase(item.getLeDeletionFlag())) {
-            if (ddUuid != null) {
-                taskletWrapper.getLeDeletionDomainWrapper().add(item);
-                log.info("Data_Distributor_LE_Deletion Record added for API processing - LE_EXTERNAL_IDENTIFIER: {}, DD_UUID: {}",
-                        item.getLeExternalIdentifier(), ddUuid);
-            } else {
-                log.warn("Data_Distributor_LE_Deletion LE External Identifier not found in mapping table, skipping API processing: {}",
-                        item.getLeExternalIdentifier());
-                errorLogDAO.logValidationError(
-                        RecordType.LEGAL_ENTITY_DELETION,
-                        item.getLeExternalIdentifier(),
-                        "LE External Identifier not found in mapping table. Record saved but skipping API processing.",
-                        null,
-                        null,
-                        fileId
-                );
-            }
-        } else {
-            log.info("Data_Distributor_LE_Deletion Record with flag 'N' saved but skipped for API - LE_EXTERNAL_IDENTIFIER: {}",
-                    item.getLeExternalIdentifier());
-        }
-
-        return item;
-    }
-
-    @Override
-    public void beforeStep(StepExecution stepExecution) {
-        log.info("Data_Distributor_LE_Deletion Preloading LE UUID mapping table into memory");
-        leUuidMap = mappingDAO.fetchAllLEUuidMappings();
-        this.fileId = stepExecution.getJobParameters().getString("leDeletionFileId");
-        log.info("Data_Distributor_LE_Deletion Loaded {} UUIDs from mapping table", leUuidMap.size());
-    }
-
-    /**
-     * Flush all valid records to DD_LEGALENTITY_DELETE_TBL
-     */
-    public void flushRecords() {
-        if (!recordsToInsert.isEmpty()) {
-            log.info("Data_Distributor_LE_Deletion Flushing {} deletion records to DB", recordsToInsert.size());
-            deletionDAO.batchInsertDeleteRecords(recordsToInsert);
-            recordsToInsert.clear();
-        }
-    }
-
-    @Override
-    public ExitStatus afterStep(StepExecution stepExecution) {
-        flushRecords();
-        log.info("Data_Distributor_LE_Deletion Step completed. Records for API processing: {}",
-                taskletWrapper.getLeDeletionDomainWrapper().size());
-        return ExitStatus.COMPLETED;
-    }
-}
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/batch/schedular/BatchSchedular.java b/DataDistribution/src/main/java/com/ing/datadist/batch/schedular/BatchSchedular.java
index e489dbc..2e68f48 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/batch/schedular/BatchSchedular.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/batch/schedular/BatchSchedular.java
@@ -31,18 +31,15 @@ public class BatchSchedular {
     private final Job cofaceOUjob;
     private final Job cofaceLEJob;
     private final Job incapablesJob;
-    private final Job leDeletionJob;
     private final JobExplorer jobExplorer;
     private final ErrorLogDAO errorLogDAO;
     private final FileIngestionService fileIngestionService;
     private final AtomicBoolean organisationUnitBatchRunning = new AtomicBoolean(false);
     private final AtomicBoolean legalEntityBatchRunning = new AtomicBoolean(false);
     private final AtomicBoolean incapablesBatchRunning = new AtomicBoolean(false);
-    private final AtomicBoolean leDeletionBatchRunning = new AtomicBoolean(false);
     Boolean runFlagOU = true;
     Boolean runFlagLE = true;
     Boolean runFlagIncapables = true;
-    Boolean runFlagLEDeletion = true;
 
 
     @Value("${spring.batch.schedular.organisationUnitCron}")
@@ -54,9 +51,6 @@ public class BatchSchedular {
     @Value("${spring.batch.schedular.incapablesCron}")
     private String incapablesCron;
 
-    @Value("${spring.batch.schedular.leDeletionCron:0 */5 * * * *}")
-    private String leDeletionCron;
-
     @Value("${ecs.cofaceIncapablesFileName}")
     private String cofaceIncapablesFileName;
 
@@ -69,9 +63,6 @@ public class BatchSchedular {
     @Value("${ecs.cofaceLEChildFileName}")
     private String cofaceLEChildFileName;
 
-    @Value("${ecs.cofaceLEDeleteFileName:COFACE_LE_DEL.csv}")
-    private String cofaceLEDeleteFileName;
-
     @Value("${ecs.readFileFrom}")
     private String readFileFrom;
 
@@ -80,12 +71,10 @@ public class BatchSchedular {
                           @Qualifier("organisationUnitEnrichmentJob") Job job,
                           @Qualifier("LegalEntityEnrichmentJob") Job cofaceLEJob,
                           @Qualifier("IncapablesEnrichmentJob") Job incapablesJob,
-                          @Qualifier("LegalEntityDeletionJob") Job leDeletionJob,
                           JobExplorer jobExplorer, ErrorLogDAO errorLogDAO, FileIngestionService fileIngestionService) {    this.jobLauncher = jobLauncher;
         this.cofaceOUjob = job;
         this.cofaceLEJob = cofaceLEJob;
         this.incapablesJob = incapablesJob;
-        this.leDeletionJob = leDeletionJob;
         this.jobExplorer = jobExplorer;
         this.errorLogDAO = errorLogDAO;
         this.fileIngestionService = fileIngestionService;
@@ -476,99 +465,4 @@ public class BatchSchedular {
         }
         runFlagIncapables = false;
     }
-
-    @Scheduled(cron = "${spring.batch.schedular.leDeletionCron:0 */5 * * * *}")
-    public void runLEDeletion() {
-        log.info("LegalEntity Deletion Batch Schedular started:{}", LocalDateTime.now());
-        JobExecution execution = null;
-        String fileId = null;
-        String fileName = null;
-
-        if (!leDeletionBatchRunning.compareAndSet(false, true)) {
-            log.info("LegalEntity Deletion Batch still running, skipping this run :{}", LocalDateTime.now());
-            return;
-        }
-        if (runFlagLEDeletion) {
-            try {
-                log.info("LegalEntity Deletion Batch Schedular cron:{} started:{}", leDeletionCron, LocalDateTime.now());
-
-                // Fetch file name from configuration
-                fileName = cofaceLEDeleteFileName;
-                String filePath = buildFilePath(fileName);
-
-                Long totalRecords = 0L;
-                Long fileSize = 0L;
-
-                fileId = fileIngestionService.registerFileIngestion(
-                        fileName,
-                        "COFACE_LE_DELETION",
-                        "LE",
-                        "COFACE_SYSTEM",
-                        "DAILY",
-                        totalRecords
-                );
-
-                JobParameters jobParameters = new JobParametersBuilder(jobExplorer)
-                        .addLong("run.id", System.currentTimeMillis())
-                        .addString("leDeletionFileId", fileId)
-                        .addString("fileName", fileName)
-                        .toJobParameters();
-
-                // Mark file processing start
-                fileIngestionService.markFileProcessingStart(fileId, String.valueOf(System.currentTimeMillis()));
-
-                execution = jobLauncher.run(leDeletionJob, jobParameters);
-
-                // Update file processing completion based on execution status
-                if (execution.getStatus().toString().equals("COMPLETED")) {
-                    log.info("LegalEntity Deletion Batch completed - fileId: {}", fileId);
-                } else {
-                    String errorMsg = "Batch execution failed with status: " + execution.getStatus();
-                    log.error("LegalEntity Deletion Batch failed: {}", errorMsg);
-                    fileIngestionService.markFileProcessingFailed(fileId, errorMsg);
-                }
-            } catch (Exception e) {
-                log.error("Exception in LegalEntity Deletion Batch Schedular", e);
-
-                // Log to DD_ERROR_LOG table
-                try {
-                    StringWriter sw = new StringWriter();
-                    e.printStackTrace(new PrintWriter(sw));
-
-                    errorLogDAO.logError(
-                        RecordType.LEGAL_ENTITY_DELETION,
-                        "BATCH_JOB",
-                        null,
-                        ErrorType.DATABASE_ERROR,
-                        "Batch Scheduler Exception: " + e.getMessage(),
-                        null,
-                        fileName,
-                        sw.toString(),
-                        fileId
-                    );
-                } catch (Exception logEx) {
-                    log.error("Failed to log batch scheduler error", logEx);
-                }
-
-                // Mark file as failed
-                if (fileId != null) {
-                    try {
-                        fileIngestionService.markFileProcessingFailed(fileId, "Exception in batch scheduler: " + e.getMessage());
-                    } catch (Exception fileEx) {
-                        log.error("Failed to mark file as failed", fileEx);
-                    }
-                }
-            } finally {
-                if (execution != null) {
-                    log.info("LegalEntity Deletion Batch Schedular finished name:{} status:{}", leDeletionJob.getName(), execution.getStatus());
-                } else {
-                    log.info("LegalEntity Deletion Batch Schedular failed name:{}", leDeletionJob.getName());
-                }
-                leDeletionBatchRunning.set(false);
-            }
-        } else {
-            log.info("LegalEntity Deletion Batch Schedular flag stopped");
-        }
-        runFlagLEDeletion = false;
-    }
 }
diff --git a/DataDistribution/src/main/java/com/ing/datadist/batch/tasklet/LEDeletionApiTasklet.java b/DataDistribution/src/main/java/com/ing/datadist/batch/tasklet/LEDeletionApiTasklet.java
deleted file mode 100644
index 4326e2f..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/batch/tasklet/LEDeletionApiTasklet.java
+++ /dev/null
@@ -1,256 +0,0 @@
-package com.ing.datadist.batch.tasklet;
-
-import com.ing.datadist.api.model.UpdateOrganisationRequest;
-import com.ing.datadist.api.model.SearchInvolvedPartiesResponseV1;
-import com.ing.datadist.api.service.InvolvedPartySearchService;
-import com.ing.datadist.api.service.OrganisationOnePamRepository;
-import com.ing.datadist.api.utils.DataDistributionConstants;
-import com.ing.datadist.api.utils.ErrorHandlingUtil;
-import com.ing.datadist.api.utils.OnePamDateUtil;
-import com.ing.datadist.api.utils.RegkeyEnum;
-import com.ing.datadist.dao.ApiTransactionLogDAO;
-import com.ing.datadist.dao.ErrorLogDAO;
-import com.ing.datadist.domain.LegalEntityDeletionDomain;
-import com.ing.datadist.domain.TaskletLeDeletionDomainWrapper;
-import com.ing.datadist.enums.ErrorType;
-import com.ing.datadist.enums.RecordType;
-import lombok.extern.slf4j.Slf4j;
-import org.springframework.batch.core.ExitStatus;
-import org.springframework.batch.core.StepContribution;
-import org.springframework.batch.core.StepExecution;
-import org.springframework.batch.core.StepExecutionListener;
-import org.springframework.batch.core.configuration.annotation.StepScope;
-import org.springframework.batch.core.scope.context.ChunkContext;
-import org.springframework.batch.core.step.tasklet.Tasklet;
-import org.springframework.batch.repeat.RepeatStatus;
-import org.springframework.beans.factory.annotation.Autowired;
-import org.springframework.stereotype.Component;
-
-import java.io.PrintWriter;
-import java.io.StringWriter;
-import java.time.LocalDate;
-import java.time.format.DateTimeFormatter;
-import java.util.List;
-import java.util.concurrent.CompletableFuture;
-
-/**
- * Tasklet for processing Legal Entity Deletion API calls
- * Calls UPDATE_INVOLVED_PARTY API to set organisationLifeCycleStatusType = DSLV_ORG
- */
-@Component
-@StepScope
-@Slf4j
-public class LEDeletionApiTasklet implements Tasklet, StepExecutionListener {
-
-    private static final String DISSOLVED_ORG_STATUS = "DSLV_ORG";
-    private static final String FLOW_NAME = "COFACE_LE_DELETION";
-
-    @Autowired
-    private TaskletLeDeletionDomainWrapper taskletWrapper;
-
-    @Autowired
-    private InvolvedPartySearchService searchService;
-
-    @Autowired
-    private OrganisationOnePamRepository onePamRepository;
-
-    @Autowired
-    private ApiTransactionLogDAO apiLogDAO;
-
-    @Autowired
-    private ErrorLogDAO errorLogDAO;
-
-    private String fileId;
-
-    @Override
-    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
-        List<LegalEntityDeletionDomain> records = taskletWrapper.getLeDeletionDomainWrapper();
-
-        if (records.isEmpty()) {
-            log.info("Data_Distributor_LE_Deletion No records to process for API calls");
-            return RepeatStatus.FINISHED;
-        }
-
-        log.info("Data_Distributor_LE_Deletion Processing {} records with deletion flag Y", records.size());
-
-        int successCount = 0;
-        int failedCount = 0;
-
-        for (LegalEntityDeletionDomain record : records) {
-            try {
-                boolean success = processRecord(record);
-                if (success) {
-                    successCount++;
-                } else {
-                    failedCount++;
-                }
-
-                // Update notification status after processing each record
-                apiLogDAO.updateNotificationStatusByUuid(record.getDdUuid(), fileId);
-
-            } catch (Exception e) {
-                log.error("Data_Distributor_LE_Deletion Error processing record: {}", record.getLeExternalIdentifier(), e);
-                logError(record, e);
-                failedCount++;
-            }
-        }
-
-        log.info("Data_Distributor_LE_Deletion Processing complete. Success: {}, Failed: {}", successCount, failedCount);
-        taskletWrapper.clear();
-        return RepeatStatus.FINISHED;
-    }
-
-    private boolean processRecord(LegalEntityDeletionDomain record) {
-        String ddUuid = record.getDdUuid();
-        String leExternalIdentifier = record.getLeExternalIdentifier();
-
-        log.info("Data_Distributor_LE_Deletion Processing record - LE_EXTERNAL_IDENTIFIER: {}, DD_UUID: {}",
-                leExternalIdentifier, ddUuid);
-
-        // Step 1: Search OnePAM to get OnePAM UUID
-        String onePamUuid = searchOrganisationByDdUuid(ddUuid);
-
-        if (onePamUuid == null) {
-            log.error("Data_Distributor_LE_Deletion OnePAM UUID not found for DD_UUID: {}", ddUuid);
-            errorLogDAO.logApiError(
-                    RecordType.LEGAL_ENTITY_DELETION,
-                    leExternalIdentifier,
-                    ddUuid,
-                    "ONEPAM_NOT_FOUND",
-                    "OnePAM UUID not found for DD_UUID: " + ddUuid,
-                    RegkeyEnum.SEARCH_INVOLVED_PARTY.endpoint,
-                    null, null, null, null, fileId
-            );
-            return false;
-        }
-
-        log.info("Data_Distributor_LE_Deletion Found OnePAM UUID: {} for DD_UUID: {}", onePamUuid, ddUuid);
-
-        // Step 2: Call UPDATE_INVOLVED_PARTY API to set lifecycle status to DSLV_ORG
-        return updateOrganisationLifecycleStatus(record, onePamUuid);
-    }
-
-    private String searchOrganisationByDdUuid(String ddUuid) {
-        try {
-            SearchInvolvedPartiesResponseV1 result = searchService.searchInvolvedPartyByInternalIdentifier(
-                    "organisation",
-                    DataDistributionConstants.DD_IDENTIFIER_TYPE,
-                    ddUuid,
-                    fileId
-            );
-
-            if (result != null && result.getOrganisations() != null && !result.getOrganisations().isEmpty()) {
-                return result.getOrganisations().get(0).getInvolvedPartyIdentifier();
-            }
-        } catch (Exception e) {
-            log.error("Data_Distributor_LE_Deletion Error searching organisation for DD_UUID: {}", ddUuid, e);
-        }
-        return null;
-    }
-
-    private boolean updateOrganisationLifecycleStatus(LegalEntityDeletionDomain record, String onePamUuid) {
-        String endpoint = RegkeyEnum.UPDATE_INVOLVED_PARTY.resolveEndpoint(onePamUuid);
-        String apiMethod = RegkeyEnum.UPDATE_INVOLVED_PARTY.method;
-
-        // Build request payload
-        UpdateOrganisationRequest.Organisation org = UpdateOrganisationRequest.Organisation.builder()
-                .organisationLifeCycleStatusType(DISSOLVED_ORG_STATUS)
-                .effectiveDate(OnePamDateUtil.toOnePamDate(LocalDate.now().format(DateTimeFormatter.ISO_DATE)))
-                .build();
-
-        UpdateOrganisationRequest request = UpdateOrganisationRequest.builder()
-                .organisation(org)
-                .build();
-
-        String requestPayload = "{\"organisation\":{\"organisationLifeCycleStatusType\":\"" + DISSOLVED_ORG_STATUS + "\"}}";
-
-        try {
-            log.info("Data_Distributor_LE_Deletion Calling UPDATE_INVOLVED_PARTY API for UUID: {}", onePamUuid);
-
-            CompletableFuture<?> futureResponse = onePamRepository.updateOrganisation(onePamUuid, request);
-            Object response = futureResponse.get();
-
-            // Log successful API call
-            apiLogDAO.logApiSuccess(
-                    record.getDdUuid(),
-                    fileId,
-                    FLOW_NAME,
-                    endpoint,
-                    apiMethod,
-                    requestPayload,
-                    200,
-                    response != null ? response.toString() : null
-            );
-
-            log.info("Data_Distributor_LE_Deletion Successfully updated lifecycle status to DSLV_ORG for UUID: {}", onePamUuid);
-            return true;
-
-        } catch (Exception e) {
-            log.error("Data_Distributor_LE_Deletion API error for UUID: {}", onePamUuid, e);
-
-            Throwable rootCause = ErrorHandlingUtil.getRootCause(e);
-            String errorMessage = rootCause.getMessage();
-
-            // Log API failure
-            apiLogDAO.logApiFailure(
-                    record.getDdUuid(),
-                    fileId,
-                    FLOW_NAME,
-                    endpoint,
-                    apiMethod,
-                    requestPayload,
-                    null,
-                    null,
-                    "API_EXCEPTION",
-                    errorMessage
-            );
-
-            // Log to error table
-            errorLogDAO.logApiError(
-                    RecordType.LEGAL_ENTITY_DELETION,
-                    record.getLeExternalIdentifier(),
-                    record.getDdUuid(),
-                    "API_EXCEPTION",
-                    errorMessage,
-                    endpoint,
-                    requestPayload,
-                    null,
-                    null,
-                    null,
-                    fileId
-            );
-
-            return false;
-        }
-    }
-
-    private void logError(LegalEntityDeletionDomain record, Exception e) {
-        StringWriter sw = new StringWriter();
-        e.printStackTrace(new PrintWriter(sw));
-
-        errorLogDAO.logError(
-                RecordType.LEGAL_ENTITY_DELETION,
-                record.getLeExternalIdentifier(),
-                record.getDdUuid(),
-                ErrorType.PROCESSING_ERROR,
-                e.getMessage(),
-                null,
-                null,
-                sw.toString(),
-                fileId
-        );
-    }
-
-    @Override
-    public void beforeStep(StepExecution stepExecution) {
-        this.fileId = stepExecution.getJobParameters().getString("leDeletionFileId");
-        log.info("Data_Distributor_LE_Deletion API Tasklet started for fileId: {}", fileId);
-    }
-
-    @Override
-    public ExitStatus afterStep(StepExecution stepExecution) {
-        log.info("Data_Distributor_LE_Deletion API Tasklet completed");
-        return ExitStatus.COMPLETED;
-    }
-}
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/dao/IncapablesAccountingDAO.java b/DataDistribution/src/main/java/com/ing/datadist/dao/IncapablesAccountingDAO.java
deleted file mode 100644
index 78947f9..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/dao/IncapablesAccountingDAO.java
+++ /dev/null
@@ -1,161 +0,0 @@
-package com.ing.datadist.dao;
-
-import com.ing.datadist.dao.dbfields.Accounting.IncapableAccountingDbFields;
-import com.ing.datadist.dao.queries.IncapablesQueries;
-import com.ing.datadist.kafka.domain.*;
-import lombok.extern.slf4j.Slf4j;
-import org.springframework.jdbc.core.JdbcTemplate;
-import org.springframework.stereotype.Repository;
-
-@Repository
-@Slf4j
-public class IncapablesAccountingDAO {
-
-    private final JdbcTemplate jdbcTemplate;
-
-    public IncapablesAccountingDAO(JdbcTemplate jdbcTemplate) {
-        this.jdbcTemplate = jdbcTemplate;
-    }
-
-    public Individual getIndividualByUUID(String ddUuid) {
-        return jdbcTemplate.query(IncapablesQueries.SELECT_INDIVIDUAL_BY_UUID, rs -> {
-            if (rs.next()) {
-                Individual individual = new Individual();
-                individual.setDdUuid(rs.getString(IncapableAccountingDbFields.DD_UUID));
-                individual.setEventId(rs.getString(IncapableAccountingDbFields.EVENT_ID));
-                individual.setGender(rs.getString(IncapableAccountingDbFields.INCP_GENDER));
-                individual.setDateOfBirth(rs.getString(IncapableAccountingDbFields.INCP_DATEOFBIRTH));
-                individual.setCityOfBirth(rs.getString(IncapableAccountingDbFields.INCP_CITYOFBIRTH));
-                individual.setCountryOfBirth(rs.getString(IncapableAccountingDbFields.INCP_COUNTRYOFBIRTH));
-                individual.setDateOfDeathDi(rs.getString(IncapableAccountingDbFields.INCP_DATEOFDEATH_DI));
-                individual.setCountryOfResidenceDi(rs.getString(IncapableAccountingDbFields.INCP_COUNTRYOFRESIDENCE_DI));
-                return individual;
-            }
-            return null;
-        }, ddUuid);
-    }
-
-    public IndividualName getIndividualNameByUUID(String ddUuid) {
-        return jdbcTemplate.query(IncapablesQueries.SELECT_INDIVIDUAL_NAME_BY_UUID, rs -> {
-            if (rs.next()) {
-                IndividualName individualName = new IndividualName();
-                individualName.setDdUuid(rs.getString(IncapableAccountingDbFields.DD_UUID));
-                individualName.setEventId(rs.getString(IncapableAccountingDbFields.EVENT_ID));
-                individualName.setLastName(rs.getString(IncapableAccountingDbFields.INCP_LASTNAME));
-                individualName.setFirstName1(rs.getString(IncapableAccountingDbFields.INCP_FIRSTNAME));
-                return individualName;
-            }
-            return null;
-        }, ddUuid);
-    }
-
-    public PostalAddress getIncapableAddressByUUID(String ddUuid) {
-        return jdbcTemplate.query(IncapablesQueries.SELECT_INCAPABLE_ADDRESS_BY_UUID, rs -> {
-            if (rs.next()) {
-                PostalAddress address = new PostalAddress();
-                address.setStreetName(rs.getString(IncapableAccountingDbFields.INCP_STREETNAME_DI));
-                address.setHouseNumber(rs.getString(IncapableAccountingDbFields.INCP_HOUSENUMBER_DI));
-                address.setHouseNumberAddition(rs.getString(IncapableAccountingDbFields.INCP_HOUSENUMBERADDITION_DI));
-                address.setPostalCode(rs.getString(IncapableAccountingDbFields.INCP_POSTALCODE_DI));
-                address.setCityName(rs.getString(IncapableAccountingDbFields.INCP_CITYNAME_DI));
-                address.setCountryCode(rs.getString(IncapableAccountingDbFields.INCP_COUNTRYOFRESIDENCE_DI));
-                return address;
-            }
-            return null;
-        }, ddUuid);
-    }
-
-    public IndividualName getAdministratorNameByUUID(String ddUuid) {
-        return jdbcTemplate.query(IncapablesQueries.SELECT_ADMINISTRATOR_BY_UUID, rs -> {
-            if (rs.next()) {
-                IndividualName administratorName = new IndividualName();
-                administratorName.setDdUuid(rs.getString(IncapableAccountingDbFields.DD_UUID));
-                administratorName.setEventId(rs.getString(IncapableAccountingDbFields.EVENT_ID));
-                administratorName.setLastName(rs.getString(IncapableAccountingDbFields.ADM_LASTNAME_DI));
-                administratorName.setFirstName1(rs.getString(IncapableAccountingDbFields.ADM_FIRSTNAME_DI));
-                return administratorName;
-            }
-            return null;
-        }, ddUuid);
-    }
-
-    public PostalAddress getAdministratorAddressByUUID(String ddUuid) {
-        return jdbcTemplate.query(IncapablesQueries.SELECT_ADMINISTRATOR_ADDRESS_BY_UUID, rs -> {
-            if (rs.next()) {
-                PostalAddress address = new PostalAddress();
-                address.setStreetName(rs.getString(IncapableAccountingDbFields.ADM_STREETNAME_DI));
-                address.setHouseNumber(rs.getString(IncapableAccountingDbFields.ADM_HOUSENUMBER_DI));
-                address.setHouseNumberAddition(rs.getString(IncapableAccountingDbFields.ADM_HOUSENUMBERADDITION_DI));
-                address.setPostalCode(rs.getString(IncapableAccountingDbFields.ADM_POSTALCODE_DI));
-                address.setCityName(rs.getString(IncapableAccountingDbFields.ADM_CITYNAME_DI));
-                address.setCountryCode(rs.getString(IncapableAccountingDbFields.ADM_COUNTRYOFRESIDENCE_DI));
-                return address;
-            }
-            return null;
-        }, ddUuid);
-    }
-
-    public Occupation getOccupationByUUID(String ddUuid) {
-        return jdbcTemplate.query(IncapablesQueries.SELECT_OCCUPATION_BY_UUID, rs -> {
-            if (rs.next()) {
-                Occupation occupation = new Occupation();
-                occupation.setDdUuid(rs.getString(IncapableAccountingDbFields.DD_UUID));
-                occupation.setEventId(rs.getString(IncapableAccountingDbFields.EVENT_ID));
-                occupation.setClassificationCode(rs.getString(IncapableAccountingDbFields.ADM_OCCUPATIONCODE_DI));
-                occupation.setRank(rs.getString(IncapableAccountingDbFields.ADM_OCCUPATIONRANK));
-                occupation.setEndDate(rs.getString(IncapableAccountingDbFields.ADM_RESPONSIBILITY_ENDDATE_DI));
-                return occupation;
-            }
-            return null;
-        }, ddUuid);
-    }
-
-    public com.ing.datadist.kafka.domain.Marking getMarkingByUUID(String ddUuid) {
-        return jdbcTemplate.query(IncapablesQueries.SELECT_MARKING_BY_UUID, rs -> {
-            if (rs.next()) {
-                com.ing.datadist.kafka.domain.Marking marking = new com.ing.datadist.kafka.domain.Marking();
-                marking.setDdUuid(rs.getString(IncapableAccountingDbFields.DD_UUID));
-                marking.setEventId(rs.getString(IncapableAccountingDbFields.EVENT_ID));
-                marking.setObjectCodeDi(rs.getString(IncapableAccountingDbFields.OBJECT_CODE_DI));
-                marking.setIncapabilityEffectiveDateDi(rs.getString(IncapableAccountingDbFields.INAB_EFFECTIVEDATE_DI));
-                marking.setIncapabilityEndDateDi(rs.getString(IncapableAccountingDbFields.INAB_ENDDATE_DI));
-                return marking;
-            }
-            return null;
-        }, ddUuid);
-    }
-
-    public IncapableDomainWrapper getCompleteIncapableByUUID(String ddUuid) {
-        return jdbcTemplate.query(IncapablesQueries.SELECT_COMPLETE_INCAPABLE_BY_UUID, rs -> {
-            if (rs.next()) {
-                IncapableDomainWrapper wrapper = new IncapableDomainWrapper();
-                wrapper.setDdUuid(rs.getString(IncapableAccountingDbFields.DD_UUID));
-                wrapper.setEventId(rs.getString(IncapableAccountingDbFields.EVENT_ID));
-
-                Individual individual = new Individual();
-                individual.setDdUuid(rs.getString(IncapableAccountingDbFields.DD_UUID));
-                individual.setEventId(rs.getString(IncapableAccountingDbFields.EVENT_ID));
-                individual.setGender(rs.getString(IncapableAccountingDbFields.INCP_GENDER));
-                individual.setDateOfBirth(rs.getString(IncapableAccountingDbFields.INCP_DATEOFBIRTH));
-                individual.setCityOfBirth(rs.getString(IncapableAccountingDbFields.INCP_CITYOFBIRTH));
-                individual.setCountryOfBirth(rs.getString(IncapableAccountingDbFields.INCP_COUNTRYOFBIRTH));
-                individual.setDateOfDeathDi(rs.getString(IncapableAccountingDbFields.INCP_DATEOFDEATH_DI));
-                individual.setCountryOfResidenceDi(rs.getString(IncapableAccountingDbFields.INCP_COUNTRYOFRESIDENCE_DI));
-                wrapper.setIndividual(individual);
-
-                IndividualName individualName = new IndividualName();
-                individualName.setLastName(rs.getString(IncapableAccountingDbFields.INCP_LASTNAME));
-                individualName.setFirstName1(rs.getString(IncapableAccountingDbFields.INCP_FIRSTNAME));
-                wrapper.setIndividualName(individualName);
-
-
-                return wrapper;
-            }
-            return null;
-        }, ddUuid);
-    }
-}
-
-
-
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/dao/LegalEntityDeletionDAO.java b/DataDistribution/src/main/java/com/ing/datadist/dao/LegalEntityDeletionDAO.java
deleted file mode 100644
index 31ed731..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/dao/LegalEntityDeletionDAO.java
+++ /dev/null
@@ -1,101 +0,0 @@
-package com.ing.datadist.dao;
-
-import com.ing.datadist.domain.LegalEntityDeletionDomain;
-import lombok.NonNull;
-import lombok.extern.slf4j.Slf4j;
-import org.springframework.beans.factory.annotation.Autowired;
-import org.springframework.jdbc.core.BatchPreparedStatementSetter;
-import org.springframework.jdbc.core.JdbcTemplate;
-import org.springframework.stereotype.Repository;
-import org.springframework.transaction.annotation.Transactional;
-
-import java.sql.PreparedStatement;
-import java.sql.SQLException;
-import java.util.List;
-
-/**
- * DAO class for Legal Entity Deletion database operations
- */
-@Repository
-@Slf4j
-public class LegalEntityDeletionDAO {
-
-    private static final String INSERT_SQL =
-            "INSERT INTO DD_LEGALENTITY_DELETE_TBL (LE_DELETION_FLAG, DD_UUID, FILE_ID, " +
-                    "CREATED_BY, CREATED_TS) VALUES (?, ?, ?, 'DD_USER', SYSTIMESTAMP)";
-
-    private static final String DELETE_BY_FILE_ID_SQL =
-            "DELETE FROM DD_LEGALENTITY_DELETE_TBL WHERE FILE_ID = ?";
-
-    @Autowired
-    private JdbcTemplate jdbcTemplate;
-
-    @Transactional
-    public void insertDeleteRecord(LegalEntityDeletionDomain record) {
-        log.info("LegalEntityDeletionDAO: Inserting deletion record for DD_UUID: {}",
-                record.getDdUuid());
-
-        try {
-            jdbcTemplate.update(INSERT_SQL,
-                    record.getLeDeletionFlag(),
-                    record.getDdUuid(),
-                    record.getFileId());
-            log.info("LegalEntityDeletionDAO: Successfully inserted deletion record for DD_UUID: {}",
-                    record.getDdUuid());
-        } catch (Exception e) {
-            log.error("LegalEntityDeletionDAO: Error inserting deletion record for DD_UUID: {}",
-                    record.getDdUuid(), e);
-            throw new RuntimeException("Failed to insert LE deletion record", e);
-        }
-    }
-
-    @Transactional
-    public void batchInsertDeleteRecords(List<LegalEntityDeletionDomain> records) {
-        log.info("LegalEntityDeletionDAO: Starting batch insert for {} records", records.size());
-
-        if (records == null || records.isEmpty()) {
-            log.warn("LegalEntityDeletionDAO: No records to insert");
-            return;
-        }
-
-        try {
-            int[] updateCounts = jdbcTemplate.batchUpdate(INSERT_SQL, new BatchPreparedStatementSetter() {
-                @Override
-                public void setValues(@NonNull PreparedStatement ps, int i) throws SQLException {
-                    LegalEntityDeletionDomain record = records.get(i);
-                    log.debug("Inserting LE deletion record {}: DD_UUID={}, FILE_ID={}",
-                            i, record.getDdUuid(), record.getFileId());
-
-                    ps.setString(1, record.getLeDeletionFlag());
-                    ps.setString(2, record.getDdUuid());
-                    ps.setString(3, record.getFileId());
-                }
-
-                @Override
-                public int getBatchSize() {
-                    return records.size();
-                }
-            });
-
-            log.info("LegalEntityDeletionDAO: Successfully inserted {} deletion records", updateCounts.length);
-
-        } catch (Exception e) {
-            log.error("LegalEntityDeletionDAO: Error occurred during batch insert", e);
-            throw new RuntimeException("Failed to insert LE deletion records", e);
-        }
-    }
-
-    @Transactional
-    public int deleteByFileId(String fileId) {
-        log.info("LegalEntityDeletionDAO: Deleting records for FILE_ID: {}", fileId);
-
-        try {
-            int deletedCount = jdbcTemplate.update(DELETE_BY_FILE_ID_SQL, fileId);
-            log.info("LegalEntityDeletionDAO: Deleted {} records for FILE_ID: {}", deletedCount, fileId);
-            return deletedCount;
-        } catch (Exception e) {
-            log.error("LegalEntityDeletionDAO: Error deleting records for FILE_ID: {}", fileId, e);
-            throw new RuntimeException("Failed to delete LE deletion records", e);
-        }
-    }
-}
\ No newline at end of file
diff --git a/DataDistribution/src/main/java/com/ing/datadist/dao/dbfields/Accounting/IncapableAccountingDbFields.java b/DataDistribution/src/main/java/com/ing/datadist/dao/dbfields/Accounting/IncapableAccountingDbFields.java
deleted file mode 100644
index 1040c3e..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/dao/dbfields/Accounting/IncapableAccountingDbFields.java
+++ /dev/null
@@ -1,87 +0,0 @@
-package com.ing.datadist.dao.dbfields.Accounting;
-
-public final class IncapableAccountingDbFields {
-
-    // Primary Keys
-    public static final String DD_UUID = "DD_UUID";
-    public static final String EVENT_ID = "EVENT_ID";
-
-    // Incapable Person - Core Fields
-    public static final String INCP_GENDER = "INCP_GENDER";
-    public static final String INCP_LASTNAME = "INCP_LASTNAME";
-    public static final String INCP_FIRSTNAME = "INCP_FIRSTNAME";
-    public static final String INCP_DATEOFBIRTH = "INCP_DATEOFBIRTH";
-    public static final String INCP_CITYOFBIRTH = "INCP_CITYOFBIRTH";
-    public static final String INCP_COUNTRYOFBIRTH = "INCP_COUNTRYOFBIRTH";
-
-    public static final String INCP_DATEOFDEATH_ONEPAM = "INCP_DATEOFDEATH_ONEPAM";
-    public static final String INCP_DATEOFDEATH_DI = "INCP_DATEOFDEATH_DI";
-
-    public static final String INCP_COUNTRYOFRESIDENCE_ONEPAM = "INCP_COUNTRYOFRESIDENCE_ONEPAM";
-    public static final String INCP_COUNTRYOFRESIDENCE_DI = "INCP_COUNTRYOFRESIDENCE_DI";
-
-    // Incapable Person - Address Fields
-    public static final String INCP_STREETNAME_ONEPAM = "INCP_STREETNAME_ONEPAM";
-    public static final String INCP_STREETNAME_DI = "INCP_STREETNAME_DI";
-
-    public static final String INCP_HOUSENUMBER_ONEPAM = "INCP_HOUSENUMBER_ONEPAM";
-    public static final String INCP_HOUSENUMBER_DI = "INCP_HOUSENUMBER_DI";
-
-    public static final String INCP_HOUSENUMBERADDITION_ONEPAM = "INCP_HOUSENUMBERADDITION_ONEPAM";
-    public static final String INCP_HOUSENUMBERADDITION_DI = "INCP_HOUSENUMBERADDITION_DI";
-
-    public static final String INCP_POSTALCODE_ONEPAM = "INCP_POSTALCODE_ONEPAM";
-    public static final String INCP_POSTALCODE_DI = "INCP_POSTALCODE_DI";
-
-    public static final String INCP_CITYNAME_ONEPAM = "INCP_CITYNAME_ONEPAM";
-    public static final String INCP_CITYNAME_DI = "INCP_CITYNAME_DI";
-
-    // Administrator - Core Fields
-    public static final String ADM_LASTNAME_ONEPAM = "ADM_LASTNAME_ONEPAM";
-    public static final String ADM_LASTNAME_DI = "ADM_LASTNAME_DI";
-
-    public static final String ADM_FIRSTNAME_ONEPAM = "ADM_FIRSTNAME_ONEPAM";
-    public static final String ADM_FIRSTNAME_DI = "ADM_FIRSTNAME_DI";
-
-    // Administrator - Address Fields
-    public static final String ADM_STREETNAME_ONEPAM = "ADM_STREETNAME_ONEPAM";
-    public static final String ADM_STREETNAME_DI = "ADM_STREETNAME_DI";
-
-    public static final String ADM_HOUSENUMBER_ONEPAM = "ADM_HOUSENUMBER_ONEPAM";
-    public static final String ADM_HOUSENUMBER_DI = "ADM_HOUSENUMBER_DI";
-
-    public static final String ADM_HOUSENUMBERADDITION_ONEPAM = "ADM_HOUSENUMBERADDITION_ONEPAM";
-    public static final String ADM_HOUSENUMBERADDITION_DI = "ADM_HOUSENUMBERADDITION_DI";
-
-    public static final String ADM_POSTALCODE_ONEPAM = "ADM_POSTALCODE_ONEPAM";
-    public static final String ADM_POSTALCODE_DI = "ADM_POSTALCODE_DI";
-
-    public static final String ADM_CITYNAME_ONEPAM = "ADM_CITYNAME_ONEPAM";
-    public static final String ADM_CITYNAME_DI = "ADM_CITYNAME_DI";
-
-    public static final String ADM_COUNTRYOFRESIDENCE_ONEPAM = "ADM_COUNTRYOFRESIDENCE_ONEPAM";
-    public static final String ADM_COUNTRYOFRESIDENCE_DI = "ADM_COUNTRYOFRESIDENCE_DI";
-
-    // Administrator - Occupation Fields
-    public static final String ADM_OCCUPATIONCODE_ONEPAM = "ADM_OCCUPATIONCODE_ONEPAM";
-    public static final String ADM_OCCUPATIONCODE_DI = "ADM_OCCUPATIONCODE_DI";
-
-    public static final String ADM_OCCUPATIONRANK = "ADM_OCCUPATIONRANK";
-
-    public static final String ADM_RESPONSIBILITY_ENDDATE_ONEPAM = "ADM_RESPONSIBILITY_ENDDATE_ONEPAM";
-    public static final String ADM_RESPONSIBILITY_ENDDATE_DI = "ADM_RESPONSIBILITY_ENDDATE_DI";
-
-    // Marking Fields
-    public static final String OBJECT_CODE_ONEPAM = "OBJECT_CODE_ONEPAM";
-    public static final String OBJECT_CODE_DI = "OBJECT_CODE_DI";
-
-    public static final String INAB_EFFECTIVEDATE_ONEPAM = "INAB_EFFECTIVEDATE_ONEPAM";
-    public static final String INAB_EFFECTIVEDATE_DI = "INAB_EFFECTIVEDATE_DI";
-
-    public static final String INAB_ENDDATE_ONEPAM = "INAB_ENDDATE_ONEPAM";
-    public static final String INAB_ENDDATE_DI = "INAB_ENDDATE_DI";
-
-    private IncapableAccountingDbFields() {
-    }
-}
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/dao/dbfields/TableNames.java b/DataDistribution/src/main/java/com/ing/datadist/dao/dbfields/TableNames.java
deleted file mode 100644
index e430e76..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/dao/dbfields/TableNames.java
+++ /dev/null
@@ -1,16 +0,0 @@
-package com.ing.datadist.dao.dbfields;
-
-
-public final class TableNames {
-
-    // Incapables related tables
-    public static final String DD_INCAPABLES_TBL = "DD_INCAPABLES_TBL";           // Legacy table
-    public static final String DD_INCAPABLES_ACCT_TBL = "DD_INCAPABLES_ACCT_TBL"; // Accounting table
-
-
-    private TableNames() {
-
-    }
-}
-
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/dao/queries/IncapablesQueries.java b/DataDistribution/src/main/java/com/ing/datadist/dao/queries/IncapablesQueries.java
index ece7b50..9b2da60 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/dao/queries/IncapablesQueries.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/dao/queries/IncapablesQueries.java
@@ -1,29 +1,30 @@
 package com.ing.datadist.dao.queries;
 
-import com.ing.datadist.dao.dbfields.IncapablesDbFields;
-import com.ing.datadist.dao.dbfields.Accounting.IncapableAccountingDbFields;
-import com.ing.datadist.dao.dbfields.TableNames;
+import static com.ing.datadist.dao.dbfields.IncapablesDbFields.*;
 
 public final class IncapablesQueries {
 
-    // INSERT query for legacy DD_INCAPABLES_TBL table (uses IncapablesDbFields)
-    public static final String INCAPABLES_TABLE_INSERT_SQL = "INSERT INTO " + TableNames.DD_INCAPABLES_TBL + " (" +
-            IncapablesDbFields.DD_UUID + ", " + IncapablesDbFields.INCP_GENDER + ", " + IncapablesDbFields.INCP_LASTNAME + ", " + IncapablesDbFields.INCP_FIRSTNAME + ", " + IncapablesDbFields.INCP_STREETNAME + ", " +
-            IncapablesDbFields.INCP_HOUSENUMBER + ", " + IncapablesDbFields.INCP_HOUSENUMBERADDITION + ", " + IncapablesDbFields.INCP_POSTALCODE + ", " + IncapablesDbFields.INCP_CITYNAME + ", " +
-            IncapablesDbFields.INCP_COUNTRYOFRESIDENCE + ", " + IncapablesDbFields.INCP_DATEOFBIRTH + ", " + IncapablesDbFields.INCP_DATEOFDEATH + ", " + IncapablesDbFields.INCP_CITYOFBIRTH + ", " +
-            IncapablesDbFields.INCP_COUNTRYOFBIRTH + ", " + IncapablesDbFields.OBJECT_CODE + ", " + IncapablesDbFields.INAB_ENDDATE + ", " + IncapablesDbFields.INAB_EFFECTIVEDATE + ", " +
-            IncapablesDbFields.ADM_OCCUPATIONCODE + ", " + IncapablesDbFields.ADM_LASTNAME + ", " + IncapablesDbFields.INCP_FIRSTNAME + ", " + IncapablesDbFields.ADM_STREETNAME + ", " +
-            IncapablesDbFields.ADM_HOUSENUMBER + ", " + IncapablesDbFields.ADM_HOUSENUMBERADDITION + ", " + IncapablesDbFields.ADM_POSTALCODE + ", " + IncapablesDbFields.ADM_CITYNAME + ", " +
-            IncapablesDbFields.ADM_COUNTRYOFRESIDENCE + ", " + IncapablesDbFields.ADM_RESPONSIBILITY_ENDDATE +
+    public static final String INCAPABLES_TABLE_INSERT_SQL = "INSERT INTO DD_INCAPABLES_TBL (" +
+            DD_UUID + ", " + INCP_GENDER + ", " + INCP_LASTNAME + ", " + INCP_FIRSTNAME + ", " + INCP_STREETNAME + ", " +
+            INCP_HOUSENUMBER + ", " + INCP_HOUSENUMBERADDITION + ", " + INCP_POSTALCODE + ", " + INCP_CITYNAME + ", " +
+            INCP_COUNTRYOFRESIDENCE + ", " + INCP_DATEOFBIRTH + ", " + INCP_DATEOFDEATH + ", " + INCP_CITYOFBIRTH + ", " +
+            INCP_COUNTRYOFBIRTH + ", " + OBJECT_CODE + ", " + INAB_ENDDATE + ", " + INAB_EFFECTIVEDATE + ", " +
+            ADM_OCCUPATIONCODE + ", " + ADM_LASTNAME + ", " + ADM_FIRSTNAME + ", " + ADM_STREETNAME + ", " +
+            ADM_HOUSENUMBER + ", " + ADM_HOUSENUMBERADDITION + ", " + ADM_POSTALCODE + ", " + ADM_CITYNAME + ", " +
+            ADM_COUNTRYOFRESIDENCE + ", " + ADM_RESPONSIBILITY_ENDDATE +
             ") VALUES (" +
+            // Root level property
             ":ddUuid, " +
+            // IncapablePerson properties (direct + nested postalAddress)
             ":incapablePerson.gender, :incapablePerson.lastName, :incapablePerson.firstName, " +
             ":incapablePerson.postalAddress.streetName, :incapablePerson.postalAddress.houseNumber, " +
             ":incapablePerson.postalAddress.houseNumberAddition, :incapablePerson.postalAddress.postalCode, " +
             ":incapablePerson.postalAddress.cityName, :incapablePerson.postalAddress.countryOfResidence, " +
             ":incapablePerson.dateOfBirth, :incapablePerson.dateOfDeath, :incapablePerson.cityOfBirth, " +
             ":incapablePerson.countryOfBirth, " +
+            // Root level properties
             ":objectCode, :inabilityEndDate, :inabilityEffectiveDate, " +
+            // Administrator properties (direct + nested postalAddress)
             ":administrator.occupationCode, :administrator.lastName, :administrator.firstName, " +
             ":administrator.postalAddress.streetName, :administrator.postalAddress.houseNumber, " +
             ":administrator.postalAddress.houseNumberAddition, :administrator.postalAddress.postalCode, " +
@@ -31,67 +32,7 @@ public final class IncapablesQueries {
             ":administrator.responsibilityEndDate" +
             ")";
 
-    // SELECT queries for DD_INCAPABLES_ACCT_TBL table (uses IncapableAccountingDbFields)
-
-    public static final String SELECT_INDIVIDUAL_BY_UUID =
-        "SELECT " + IncapableAccountingDbFields.DD_UUID + ", " + IncapableAccountingDbFields.EVENT_ID + ", " +
-        IncapableAccountingDbFields.INCP_GENDER + ", " + IncapableAccountingDbFields.INCP_LASTNAME + ", " + IncapableAccountingDbFields.INCP_FIRSTNAME + ", " +
-        IncapableAccountingDbFields.INCP_DATEOFBIRTH + ", " + IncapableAccountingDbFields.INCP_CITYOFBIRTH + ", " + IncapableAccountingDbFields.INCP_COUNTRYOFBIRTH + ", " +
-        IncapableAccountingDbFields.INCP_DATEOFDEATH_DI + ", " + IncapableAccountingDbFields.INCP_COUNTRYOFRESIDENCE_DI +
-        " FROM " + TableNames.DD_INCAPABLES_ACCT_TBL + " WHERE " + IncapableAccountingDbFields.DD_UUID + " = ?";
-
-    public static final String SELECT_INDIVIDUAL_NAME_BY_UUID =
-        "SELECT " + IncapableAccountingDbFields.DD_UUID + ", " + IncapableAccountingDbFields.EVENT_ID + ", " +
-        IncapableAccountingDbFields.INCP_LASTNAME + ", " + IncapableAccountingDbFields.INCP_FIRSTNAME +
-        " FROM " + TableNames.DD_INCAPABLES_ACCT_TBL + " WHERE " + IncapableAccountingDbFields.DD_UUID + " = ?";
-
-    public static final String SELECT_INCAPABLE_ADDRESS_BY_UUID =
-        "SELECT " +
-        IncapableAccountingDbFields.INCP_STREETNAME_DI + ", " + IncapableAccountingDbFields.INCP_HOUSENUMBER_DI + ", " +
-        IncapableAccountingDbFields.INCP_HOUSENUMBERADDITION_DI + ", " + IncapableAccountingDbFields.INCP_POSTALCODE_DI + ", " +
-        IncapableAccountingDbFields.INCP_CITYNAME_DI + ", " + IncapableAccountingDbFields.INCP_COUNTRYOFRESIDENCE_DI +
-        " FROM " + TableNames.DD_INCAPABLES_ACCT_TBL + " WHERE " + IncapableAccountingDbFields.DD_UUID + " = ?";
-
-    public static final String SELECT_ADMINISTRATOR_BY_UUID =
-        "SELECT " + IncapableAccountingDbFields.DD_UUID + ", " + IncapableAccountingDbFields.EVENT_ID + ", " +
-        IncapableAccountingDbFields.ADM_LASTNAME_DI + ", " + IncapableAccountingDbFields.ADM_FIRSTNAME_DI +
-        " FROM " + TableNames.DD_INCAPABLES_ACCT_TBL + " WHERE " + IncapableAccountingDbFields.DD_UUID + " = ?";
-
-    public static final String SELECT_ADMINISTRATOR_ADDRESS_BY_UUID =
-        "SELECT " +
-        IncapableAccountingDbFields.ADM_STREETNAME_DI + ", " + IncapableAccountingDbFields.ADM_HOUSENUMBER_DI + ", " +
-        IncapableAccountingDbFields.ADM_HOUSENUMBERADDITION_DI + ", " + IncapableAccountingDbFields.ADM_POSTALCODE_DI + ", " +
-        IncapableAccountingDbFields.ADM_CITYNAME_DI + ", " + IncapableAccountingDbFields.ADM_COUNTRYOFRESIDENCE_DI +
-        " FROM " + TableNames.DD_INCAPABLES_ACCT_TBL + " WHERE " + IncapableAccountingDbFields.DD_UUID + " = ?";
-
-    public static final String SELECT_OCCUPATION_BY_UUID =
-        "SELECT " + IncapableAccountingDbFields.DD_UUID + ", " + IncapableAccountingDbFields.EVENT_ID + ", " +
-        IncapableAccountingDbFields.ADM_OCCUPATIONCODE_DI + ", " + IncapableAccountingDbFields.ADM_OCCUPATIONRANK + ", " +
-        IncapableAccountingDbFields.ADM_RESPONSIBILITY_ENDDATE_DI +
-        " FROM " + TableNames.DD_INCAPABLES_ACCT_TBL + " WHERE " + IncapableAccountingDbFields.DD_UUID + " = ?";
-
-    public static final String SELECT_MARKING_BY_UUID =
-        "SELECT " + IncapableAccountingDbFields.DD_UUID + ", " + IncapableAccountingDbFields.EVENT_ID + ", " +
-        IncapableAccountingDbFields.OBJECT_CODE_DI + ", " + IncapableAccountingDbFields.INAB_EFFECTIVEDATE_DI + ", " + IncapableAccountingDbFields.INAB_ENDDATE_DI +
-        " FROM " + TableNames.DD_INCAPABLES_ACCT_TBL + " WHERE " + IncapableAccountingDbFields.DD_UUID + " = ?";
-
-    public static final String SELECT_COMPLETE_INCAPABLE_BY_UUID =
-        "SELECT " + IncapableAccountingDbFields.DD_UUID + ", " + IncapableAccountingDbFields.EVENT_ID + ", " +
-        IncapableAccountingDbFields.INCP_GENDER + ", " + IncapableAccountingDbFields.INCP_LASTNAME + ", " + IncapableAccountingDbFields.INCP_FIRSTNAME + ", " +
-        IncapableAccountingDbFields.INCP_DATEOFBIRTH + ", " + IncapableAccountingDbFields.INCP_CITYOFBIRTH + ", " + IncapableAccountingDbFields.INCP_COUNTRYOFBIRTH + ", " +
-        IncapableAccountingDbFields.INCP_DATEOFDEATH_DI + ", " + IncapableAccountingDbFields.INCP_COUNTRYOFRESIDENCE_DI + ", " +
-        IncapableAccountingDbFields.INCP_STREETNAME_DI + ", " + IncapableAccountingDbFields.INCP_HOUSENUMBER_DI + ", " +
-        IncapableAccountingDbFields.INCP_HOUSENUMBERADDITION_DI + ", " + IncapableAccountingDbFields.INCP_POSTALCODE_DI + ", " +
-        IncapableAccountingDbFields.INCP_CITYNAME_DI + ", " +
-        IncapableAccountingDbFields.ADM_LASTNAME_DI + ", " + IncapableAccountingDbFields.ADM_FIRSTNAME_DI + ", " +
-        IncapableAccountingDbFields.ADM_STREETNAME_DI + ", " + IncapableAccountingDbFields.ADM_HOUSENUMBER_DI + ", " +
-        IncapableAccountingDbFields.ADM_HOUSENUMBERADDITION_DI + ", " + IncapableAccountingDbFields.ADM_POSTALCODE_DI + ", " +
-        IncapableAccountingDbFields.ADM_CITYNAME_DI + ", " + IncapableAccountingDbFields.ADM_COUNTRYOFRESIDENCE_DI + ", " +
-        IncapableAccountingDbFields.ADM_OCCUPATIONCODE_DI + ", " + IncapableAccountingDbFields.ADM_OCCUPATIONRANK + ", " +
-        IncapableAccountingDbFields.ADM_RESPONSIBILITY_ENDDATE_DI + ", " +
-        IncapableAccountingDbFields.OBJECT_CODE_DI + ", " + IncapableAccountingDbFields.INAB_EFFECTIVEDATE_DI + ", " + IncapableAccountingDbFields.INAB_ENDDATE_DI +
-        " FROM " + TableNames.DD_INCAPABLES_ACCT_TBL + " WHERE " + IncapableAccountingDbFields.DD_UUID + " = ?";
-
     private IncapablesQueries() {
+        // Private constructor to prevent instantiation
     }
 }
diff --git a/DataDistribution/src/main/java/com/ing/datadist/domain/LegalEntityDeletionDomain.java b/DataDistribution/src/main/java/com/ing/datadist/domain/LegalEntityDeletionDomain.java
deleted file mode 100644
index 508edee..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/domain/LegalEntityDeletionDomain.java
+++ /dev/null
@@ -1,16 +0,0 @@
-package com.ing.datadist.domain;
-
-import lombok.Data;
-
-/**
- * Domain class for Legal Entity Deletion records from COFACE_LE_DEL.csv
- */
-@Data
-public class LegalEntityDeletionDomain {
-    private Long id;
-    private String leExternalIdentifier;
-    private String leDeletionFlag;
-    private String ddUuid;
-    private String fileId;
-}
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/domain/TaskletLeDeletionDomainWrapper.java b/DataDistribution/src/main/java/com/ing/datadist/domain/TaskletLeDeletionDomainWrapper.java
deleted file mode 100644
index 0fc4c17..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/domain/TaskletLeDeletionDomainWrapper.java
+++ /dev/null
@@ -1,24 +0,0 @@
-package com.ing.datadist.domain;
-
-import lombok.Data;
-import org.springframework.batch.core.configuration.annotation.JobScope;
-import org.springframework.stereotype.Component;
-
-import java.util.ArrayList;
-import java.util.List;
-
-/**
- * Wrapper class for Legal Entity Deletion batch processing
- */
-@Component
-@JobScope
-@Data
-public class TaskletLeDeletionDomainWrapper {
-
-    private List<LegalEntityDeletionDomain> leDeletionDomainWrapper = new ArrayList<>();
-
-    public void clear() {
-        leDeletionDomainWrapper.clear();
-    }
-}
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/enums/RecordType.java b/DataDistribution/src/main/java/com/ing/datadist/enums/RecordType.java
index 8d225cf..524c52f 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/enums/RecordType.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/enums/RecordType.java
@@ -10,9 +10,7 @@ public enum RecordType {
 
     INVOLVED_PARTY("INVOLVED_PARTY", "Involved Party"),
 
-    FILE_PROCESSING("FILE_PROCESSING", "File Processing / OIL File Creation"),
-
-    LEGAL_ENTITY_DELETION("LEGAL_ENTITY_DELETION", "Legal Entity Deletion");
+    FILE_PROCESSING("FILE_PROCESSING", "File Processing / OIL File Creation");
 
     private final String code;
     private final String description;
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/common/PostalAddressService.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/common/PostalAddressService.java
index 98627b2..b1a936c 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/common/PostalAddressService.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/common/PostalAddressService.java
@@ -73,21 +73,11 @@ public class PostalAddressService {
                 }
                 String flowType = notificationProcessingUtil.getFlowType(involvedPartyIdentifier);
                 log.debug("Determined flowType: {}", flowType);
-
-                NotificationType notificationType;
-                if (FlowType.LE.getLabel().equals(flowType)) {
-                    notificationType = NotificationType.LEGAL_ENTITY_POSTAL_ADDRESS;
-                } else if (FlowType.INCAPABLE.getLabel().equals(flowType)) {
-                    notificationType = NotificationType.INCAPABLE_INDIVIDUAL_POSTAL_ADDRESS;
-                } else {
-                    notificationType = NotificationType.ORGANISATION_UNIT_POSTAL_ADDRESS;
-                }
-
                 notificationProcessingUtil.processNotification(dataSource,
                         postalAddressBefore,
                         postalAddressAfter,
                         involvedPartyIdentifier,
-                        notificationType,
+                        FlowType.LE.getLabel().equals(flowType) ? NotificationType.LEGAL_ENTITY_POSTAL_ADDRESS : NotificationType.ORGANISATION_UNIT_POSTAL_ADDRESS,
                         transactionType
                 );
 
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/incapable/IncapableIndividualConsumerService.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/incapable/IncapableIndividualConsumerService.java
deleted file mode 100644
index 7f261c5..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/incapable/IncapableIndividualConsumerService.java
+++ /dev/null
@@ -1,96 +0,0 @@
-package com.ing.datadist.kafka.consumer.service.incapable;
-
-import com.ing.datadist.kafka.util.EventTransactionType;
-import com.ing.datadist.kafka.util.NotificationProcessingUtil;
-import com.ing.datadist.kafka.util.NotificationType;
-import com.ing.postmen.onepam.avro.model.individual.*;
-import jakarta.annotation.PostConstruct;
-import lombok.RequiredArgsConstructor;
-import lombok.extern.slf4j.Slf4j;
-import org.apache.kafka.clients.consumer.ConsumerRecord;
-import org.springframework.kafka.annotation.KafkaListener;
-import org.springframework.stereotype.Service;
-
-@Slf4j
-@Service
-@RequiredArgsConstructor
-public class IncapableIndividualConsumerService {
-
-    private final NotificationProcessingUtil notificationProcessingUtil;
-
-    @PostConstruct
-    public void postConstruct() {
-        log.info("IncapableIndividualConsumerService is created");
-    }
-
-    @KafkaListener(
-            topics = "${kafka.consumers.incapable-individual.topic-name}",
-            groupId = "${kafka.consumers.group-id}",
-            containerFactory = "kafkaListenerContainerFactory")
-    public void handleIncapableIndividual(ConsumerRecord<String, NotifyIndividual5Event> record) {
-        try {
-            log.info("Received message from topic='{}' with value='{}'", record.topic(), record.value());
-            NotifyIndividual5Event value = record.value();
-
-            if (notificationProcessingUtil.shouldProcessNotification(value.getHeader().getProperties())) {
-                Body body = value.getBody();
-
-                Before before = (body != null && body.getBefore() != null) ? body.getBefore() : null;
-                After after = (body != null && body.getAfter() != null) ? body.getAfter() : null;
-
-                Individual individualBefore = (before != null && before.getIndividual() != null) ? before.getIndividual() : null;
-                Individual individualAfter = (after != null && after.getIndividual() != null) ? after.getIndividual() : null;
-
-                String transactionType = notificationProcessingUtil.extractPropertyValue(
-                        value.getHeader().getProperties(),
-                        com.ing.datadist.kafka.util.NotificationPropertyNames.TRANSACTION_TYPE);
-
-                String involvedPartyIdentifier;
-                String dataSource;
-
-                // Determine operation based on transaction type using centralized utility methods
-                if (EventTransactionType.isAddOperation(transactionType)) {
-                    log.info("Incapable Individual is created - Transaction Type: {}", transactionType);
-                    involvedPartyIdentifier = individualAfter != null
-                            ? notificationProcessingUtil.extractDDInternalIdentifier(individualAfter.getInternalIdentifiers()) : "";
-                    dataSource = individualAfter != null ? individualAfter.getDataSource() : "";
-
-                } else if (EventTransactionType.isDeleteOperation(transactionType)) {
-                    log.info("Incapable Individual is deleted - Transaction Type: {}", transactionType);
-                    involvedPartyIdentifier = individualBefore != null
-                            ? notificationProcessingUtil.extractDDInternalIdentifier(individualBefore.getInternalIdentifiers()) : "";
-                    dataSource = individualBefore != null ? individualBefore.getDataSource() : "";
-
-                } else if (EventTransactionType.isUpdateOperation(transactionType)) {
-                    log.info("Incapable Individual is updated - Transaction Type: {}", transactionType);
-                    involvedPartyIdentifier = individualBefore != null
-                            ? notificationProcessingUtil.extractDDInternalIdentifier(individualBefore.getInternalIdentifiers()) : "";
-                    dataSource = individualAfter != null ? individualAfter.getDataSource() :
-                            (individualBefore != null ? individualBefore.getDataSource() : "");
-                } else {
-                    log.warn("Unknown transaction type: {} - Defaulting to update logic", transactionType);
-                    involvedPartyIdentifier = individualBefore != null
-                            ? notificationProcessingUtil.extractDDInternalIdentifier(individualBefore.getInternalIdentifiers()) : "";
-                    dataSource = individualAfter != null ? individualAfter.getDataSource() :
-                            (individualBefore != null ? individualBefore.getDataSource() : "");
-                }
-
-                notificationProcessingUtil.processNotification(
-                        dataSource,
-                        individualBefore,
-                        individualAfter,
-                        involvedPartyIdentifier,
-                        NotificationType.INCAPABLE_INDIVIDUAL,
-                        transactionType
-                );
-            }
-
-        } catch (Exception e) {
-            log.error("NotifyIndividual5Event event consumer failed: {}", e.getMessage(), e);
-        }
-    }
-}
-
-
-
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/incapable/IncapableIndividualNameConsumerService.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/incapable/IncapableIndividualNameConsumerService.java
deleted file mode 100644
index c3d5ed9..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/incapable/IncapableIndividualNameConsumerService.java
+++ /dev/null
@@ -1,102 +0,0 @@
-package com.ing.datadist.kafka.consumer.service.incapable;
-
-import com.ing.datadist.kafka.util.EventTransactionType;
-import com.ing.datadist.kafka.util.NotificationProcessingUtil;
-import com.ing.datadist.kafka.util.NotificationType;
-import com.ing.postmen.onepam.avro.model.individual.name.*;
-import jakarta.annotation.PostConstruct;
-import lombok.RequiredArgsConstructor;
-import lombok.extern.slf4j.Slf4j;
-import org.apache.kafka.clients.consumer.ConsumerRecord;
-import org.springframework.kafka.annotation.KafkaListener;
-import org.springframework.stereotype.Service;
-
-@Slf4j
-@Service
-@RequiredArgsConstructor
-public class IncapableIndividualNameConsumerService {
-
-    private final NotificationProcessingUtil notificationProcessingUtil;
-
-    @PostConstruct
-    public void postConstruct() {
-        log.info("IncapableIndividualNameConsumerService is created");
-    }
-
-    @KafkaListener(
-            topics = "${kafka.consumers.incapable-individual-name.topic-name}",
-            groupId = "${kafka.consumers.group-id}",
-            containerFactory = "kafkaListenerContainerFactory")
-    public void handleIncapableIndividualName(ConsumerRecord<String, NotifyIndividualName5Event> record) {
-        try {
-            log.info("Received message from topic='{}' with value='{}'", record.topic(), record.value());
-            NotifyIndividualName5Event value = record.value();
-
-            if (notificationProcessingUtil.shouldProcessNotification(value.getHeader().getProperties())) {
-                Body body = value.getBody();
-
-                Before before = (body != null && body.getBefore() != null) ? body.getBefore() : null;
-                After after = (body != null && body.getAfter() != null) ? body.getAfter() : null;
-
-                IndividualName individualNameAfter = (after != null && after.getIndividual() != null && after.getIndividual().getIndividualName() != null)
-                        ? after.getIndividual().getIndividualName() : null;
-
-                IndividualName individualNameBefore = (before != null && before.getIndividual() != null && before.getIndividual().getIndividualName() != null)
-                        ? before.getIndividual().getIndividualName() : null;
-
-                String transactionType = notificationProcessingUtil.extractPropertyValue(
-                        value.getHeader().getProperties(),
-                        com.ing.datadist.kafka.util.NotificationPropertyNames.TRANSACTION_TYPE);
-
-                String involvedPartyIdentifier;
-                String dataSource;
-
-                // Determine operation based on transaction type using centralized utility methods
-                if (EventTransactionType.isAddOperation(transactionType)) {
-                    log.info("Incapable Individual Name is created - Transaction Type: {}", transactionType);
-                    involvedPartyIdentifier = after != null && after.getIndividual() != null
-                            ? notificationProcessingUtil.extractDDInternalIdentifier(after.getIndividual().getInternalIdentifiers()) : "";
-                    dataSource = individualNameAfter != null ? individualNameAfter.getDataSource() : "";
-
-                } else if (EventTransactionType.isDeleteOperation(transactionType)) {
-                    log.info("Incapable Individual Name is deleted - Transaction Type: {}", transactionType);
-                    involvedPartyIdentifier = before != null && before.getIndividual() != null
-                            ? notificationProcessingUtil.extractDDInternalIdentifier(before.getIndividual().getInternalIdentifiers()) : "";
-                    dataSource = individualNameBefore != null ? individualNameBefore.getDataSource() : "";
-
-                } else if (EventTransactionType.isUpdateOperation(transactionType)) {
-                    log.info("Incapable Individual Name is updated - Transaction Type: {}", transactionType);
-                    involvedPartyIdentifier = before != null && before.getIndividual() != null
-                            ? notificationProcessingUtil.extractDDInternalIdentifier(before.getIndividual().getInternalIdentifiers()) : "";
-                    dataSource = individualNameAfter != null ? individualNameAfter.getDataSource() :
-                            (individualNameBefore != null ? individualNameBefore.getDataSource() : "");
-                } else {
-                    log.warn("Unknown transaction type: {} - Defaulting to update logic", transactionType);
-                    involvedPartyIdentifier = before != null && before.getIndividual() != null
-                            ? notificationProcessingUtil.extractDDInternalIdentifier(before.getIndividual().getInternalIdentifiers()) : "";
-                    dataSource = individualNameAfter != null ? individualNameAfter.getDataSource() :
-                            (individualNameBefore != null ? individualNameBefore.getDataSource() : "");
-                }
-
-                notificationProcessingUtil.processNotification(
-                        dataSource,
-                        individualNameBefore,
-                        individualNameAfter,
-                        involvedPartyIdentifier,
-                        NotificationType.INCAPABLE_INDIVIDUAL_NAME,
-                        transactionType
-                );
-            }
-        } catch (Exception e) {
-            log.error("NotifyIndividualName5Event event consumer failed: {}", e.getMessage(), e);
-        }
-    }
-}
-
-
-
-
-
-
-
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/incapable/IncapableMarkingConsumerService.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/incapable/IncapableMarkingConsumerService.java
deleted file mode 100644
index 98b7dc4..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/incapable/IncapableMarkingConsumerService.java
+++ /dev/null
@@ -1,103 +0,0 @@
-package com.ing.datadist.kafka.consumer.service.incapable;
-
-import com.ing.datadist.kafka.util.EventTransactionType;
-import com.ing.datadist.kafka.util.NotificationProcessingUtil;
-import com.ing.datadist.kafka.util.NotificationType;
-import com.ing.postmen.onepam.avro.model.marking.*;
-import jakarta.annotation.PostConstruct;
-import lombok.RequiredArgsConstructor;
-import lombok.extern.slf4j.Slf4j;
-import org.apache.kafka.clients.consumer.ConsumerRecord;
-import org.springframework.kafka.annotation.KafkaListener;
-import org.springframework.stereotype.Service;
-
-@Slf4j
-@Service
-@RequiredArgsConstructor
-public class IncapableMarkingConsumerService {
-
-    private final NotificationProcessingUtil notificationProcessingUtil;
-
-    @PostConstruct
-    public void postConstruct() {
-        log.info("IncapableMarkingConsumerService is created");
-    }
-
-    @KafkaListener(
-            topics = "${kafka.consumers.incapable-marking.topic-name}",
-            groupId = "${kafka.consumers.group-id}",
-            containerFactory = "kafkaListenerContainerFactory")
-    public void handleIncapableMarking(ConsumerRecord<String, NotifyInvolvedPartyMarking5Event> record) {
-        try {
-            log.info("Received message from topic='{}' with value='{}'", record.topic(), record.value());
-            NotifyInvolvedPartyMarking5Event value = record.value();
-
-            if (notificationProcessingUtil.shouldProcessNotification(value.getHeader().getProperties())) {
-                Body body = value.getBody();
-
-                Before before = (body != null && body.getBefore() != null) ? body.getBefore() : null;
-                After after = (body != null && body.getAfter() != null) ? body.getAfter() : null;
-
-                InvolvedPartyMarking markingAfter = (after != null && after.getInvolvedParty() != null && after.getInvolvedParty().getMarking() != null)
-                        ? after.getInvolvedParty().getMarking() : null;
-
-                InvolvedPartyMarking markingBefore = (before != null && before.getInvolvedParty() != null && before.getInvolvedParty().getMarking() != null)
-                        ? before.getInvolvedParty().getMarking() : null;
-
-                String transactionType = notificationProcessingUtil.extractPropertyValue(
-                        value.getHeader().getProperties(),
-                        com.ing.datadist.kafka.util.NotificationPropertyNames.TRANSACTION_TYPE);
-
-                String involvedPartyIdentifier;
-                String dataSource;
-
-                // Determine operation based on transaction type using centralized utility methods
-                if (EventTransactionType.isAddOperation(transactionType)) {
-                    log.info("Incapable Marking is created - Transaction Type: {}", transactionType);
-                    involvedPartyIdentifier = after != null && after.getInvolvedParty() != null
-                            ? notificationProcessingUtil.extractDDInternalIdentifier(after.getInvolvedParty().getInternalIdentifiers()) : "";
-                    dataSource = markingAfter != null ? markingAfter.getDataSource() : "";
-
-                } else if (EventTransactionType.isDeleteOperation(transactionType)) {
-                    log.info("Incapable Marking is deleted - Transaction Type: {}", transactionType);
-                    involvedPartyIdentifier = before != null && before.getInvolvedParty() != null
-                            ? notificationProcessingUtil.extractDDInternalIdentifier(before.getInvolvedParty().getInternalIdentifiers()) : "";
-                    dataSource = markingBefore != null ? markingBefore.getDataSource() : "";
-
-                } else if (EventTransactionType.isUpdateOperation(transactionType)) {
-                    log.info("Incapable Marking is updated - Transaction Type: {}", transactionType);
-                    involvedPartyIdentifier = before != null && before.getInvolvedParty() != null
-                            ? notificationProcessingUtil.extractDDInternalIdentifier(before.getInvolvedParty().getInternalIdentifiers()) : "";
-                    dataSource = markingAfter != null ? markingAfter.getDataSource() :
-                            (markingBefore != null ? markingBefore.getDataSource() : "");
-                } else {
-                    log.warn("Unknown transaction type: {} - Defaulting to update logic", transactionType);
-                    involvedPartyIdentifier = before != null && before.getInvolvedParty() != null
-                            ? notificationProcessingUtil.extractDDInternalIdentifier(before.getInvolvedParty().getInternalIdentifiers()) : "";
-                    dataSource = markingAfter != null ? markingAfter.getDataSource() :
-                            (markingBefore != null ? markingBefore.getDataSource() : "");
-                }
-
-                notificationProcessingUtil.processNotification(
-                        dataSource,
-                        markingBefore,
-                        markingAfter,
-                        involvedPartyIdentifier,
-                        NotificationType.INCAPABLE_INDIVIDUAL_IP_MARKING,
-                        transactionType
-                );
-            }
-        } catch (Exception e) {
-            log.error("NotifyInvolvedPartyMarking5Event event consumer failed for incapables: {}", e.getMessage(), e);
-        }
-    }
-}
-
-
-
-
-
-
-
-
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/incapable/IncapableOccupationConsumerService.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/incapable/IncapableOccupationConsumerService.java
deleted file mode 100644
index bb3f31b..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/incapable/IncapableOccupationConsumerService.java
+++ /dev/null
@@ -1,95 +0,0 @@
-package com.ing.datadist.kafka.consumer.service.incapable;
-
-import com.ing.datadist.api.utils.DataDistributionConstants;
-import com.ing.datadist.kafka.util.EventTransactionType;
-import com.ing.datadist.kafka.util.NotificationProcessingUtil;
-import com.ing.datadist.kafka.util.NotificationType;
-import com.ing.postmen.onepam.avro.model.individual.occupation.*;
-import jakarta.annotation.PostConstruct;
-import lombok.RequiredArgsConstructor;
-import lombok.extern.slf4j.Slf4j;
-import org.apache.kafka.clients.consumer.ConsumerRecord;
-import org.springframework.kafka.annotation.KafkaListener;
-import org.springframework.stereotype.Service;
-
-@Slf4j
-@Service
-@RequiredArgsConstructor
-public class IncapableOccupationConsumerService {
-
-    private final NotificationProcessingUtil notificationProcessingUtil;
-
-    @PostConstruct
-    public void postConstruct() {
-        log.info("IncapableOccupationConsumerService is created");
-    }
-
-    @KafkaListener(
-            topics = "${kafka.consumers.incapable-occupation.topic-name}",
-            groupId = "${kafka.consumers.group-id}",
-            containerFactory = "kafkaListenerContainerFactory")
-    public void handleIncapableOccupation(ConsumerRecord<String, NotifyOccupation5Event> record) {
-        try {
-            log.info("Received message from topic='{}' with value='{}'", record.topic(), record.value());
-            NotifyOccupation5Event value = record.value();
-
-            if (notificationProcessingUtil.shouldProcessNotification(value.getHeader().getProperties())) {
-                Body body = value.getBody();
-
-                Before before = (body != null && body.getBefore() != null) ? body.getBefore() : null;
-                After after = (body != null && body.getAfter() != null) ? body.getAfter() : null;
-
-                Occupation occupationAfter = (after != null && after.getIndividual() != null && after.getIndividual().getOccupation() != null)
-                        ? after.getIndividual().getOccupation() : null;
-
-                Occupation occupationBefore = (before != null && before.getIndividual() != null && before.getIndividual().getOccupation() != null)
-                        ? before.getIndividual().getOccupation() : null;
-
-                String transactionType = notificationProcessingUtil.extractPropertyValue(
-                        value.getHeader().getProperties(),
-                        com.ing.datadist.kafka.util.NotificationPropertyNames.TRANSACTION_TYPE);
-
-                String involvedPartyIdentifier;
-                String dataSource = DataDistributionConstants.DATA_SOURCE;
-
-                // Determine operation based on transaction type using centralized utility methods
-                if (EventTransactionType.isAddOperation(transactionType)) {
-                    log.info("Incapable Occupation is created - Transaction Type: {}", transactionType);
-                    involvedPartyIdentifier = after != null && after.getIndividual() != null
-                            ? notificationProcessingUtil.extractDDInternalIdentifier(after.getIndividual().getInternalIdentifiers()) : "";
-
-                } else if (EventTransactionType.isDeleteOperation(transactionType)) {
-                    log.info("Incapable Occupation is deleted - Transaction Type: {}", transactionType);
-                    involvedPartyIdentifier = before != null && before.getIndividual() != null
-                            ? notificationProcessingUtil.extractDDInternalIdentifier(before.getIndividual().getInternalIdentifiers()) : "";
-
-                } else if (EventTransactionType.isUpdateOperation(transactionType)) {
-                    log.info("Incapable Occupation is updated - Transaction Type: {}", transactionType);
-                    involvedPartyIdentifier = before != null && before.getIndividual() != null
-                            ? notificationProcessingUtil.extractDDInternalIdentifier(before.getIndividual().getInternalIdentifiers()) : "";
-                } else {
-                    log.warn("Unknown transaction type: {} - Defaulting to update logic", transactionType);
-                    involvedPartyIdentifier = before != null && before.getIndividual() != null
-                            ? notificationProcessingUtil.extractDDInternalIdentifier(before.getIndividual().getInternalIdentifiers()) : "";
-                }
-
-                notificationProcessingUtil.processNotification(
-                        dataSource,
-                        occupationBefore,
-                        occupationAfter,
-                        involvedPartyIdentifier,
-                        NotificationType.INCAPABLE_INDIVIDUAL_OCCUPATION,
-                        transactionType
-                );
-            }
-        } catch (Exception e) {
-            log.error("NotifyOccupation5Event event consumer failed for incapables: {}", e.getMessage(), e);
-        }
-    }
-}
-
-
-
-
-
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/involvedparty/InvolvedPartyExternalIdentifierConsumerService.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/ip/InvolvedPartyExternalIdentifierConsumerService.java
similarity index 98%
rename from DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/involvedparty/InvolvedPartyExternalIdentifierConsumerService.java
rename to DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/ip/InvolvedPartyExternalIdentifierConsumerService.java
index 5a3d7b8..72a9a6e 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/involvedparty/InvolvedPartyExternalIdentifierConsumerService.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/ip/InvolvedPartyExternalIdentifierConsumerService.java
@@ -1,4 +1,4 @@
-package com.ing.datadist.kafka.consumer.service.involvedparty;
+package com.ing.datadist.kafka.consumer.service.ip;
 
 
 import com.ing.datadist.api.utils.DataDistributionConstants;
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/involvedparty/InvolvedPartyInternalIdentifierConsumerService.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/ip/InvolvedPartyInternalIdentifierConsumerService.java
similarity index 98%
rename from DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/involvedparty/InvolvedPartyInternalIdentifierConsumerService.java
rename to DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/ip/InvolvedPartyInternalIdentifierConsumerService.java
index 80cb608..42931df 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/involvedparty/InvolvedPartyInternalIdentifierConsumerService.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/ip/InvolvedPartyInternalIdentifierConsumerService.java
@@ -1,4 +1,4 @@
-package com.ing.datadist.kafka.consumer.service.involvedparty;
+package com.ing.datadist.kafka.consumer.service.ip;
 
 
 import com.ing.datadist.dao.MappingDAO;
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/legalentity/AssessmentConsumerService.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/le/AssessmentConsumerService.java
similarity index 98%
rename from DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/legalentity/AssessmentConsumerService.java
rename to DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/le/AssessmentConsumerService.java
index 8b827ff..298500b 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/legalentity/AssessmentConsumerService.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/le/AssessmentConsumerService.java
@@ -1,4 +1,4 @@
-package com.ing.datadist.kafka.consumer.service.legalentity;
+package com.ing.datadist.kafka.consumer.service.le;
 
 import com.ing.datadist.kafka.util.NotificationProcessingUtil;
 import com.ing.datadist.kafka.util.NotificationType;
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/legalentity/OrganisationConsumerService.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/le/OrganisationConsumerService.java
similarity index 98%
rename from DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/legalentity/OrganisationConsumerService.java
rename to DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/le/OrganisationConsumerService.java
index f6ac1a3..1d2ddc2 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/legalentity/OrganisationConsumerService.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/le/OrganisationConsumerService.java
@@ -1,4 +1,4 @@
-package com.ing.datadist.kafka.consumer.service.legalentity;
+package com.ing.datadist.kafka.consumer.service.le;
 
 
 import com.ing.datadist.kafka.util.NotificationProcessingUtil;
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/legalentity/OrganisationNameConsumerService.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/le/OrganisationNameConsumerService.java
similarity index 98%
rename from DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/legalentity/OrganisationNameConsumerService.java
rename to DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/le/OrganisationNameConsumerService.java
index 048bb92..379aefd 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/legalentity/OrganisationNameConsumerService.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/le/OrganisationNameConsumerService.java
@@ -1,4 +1,4 @@
-package com.ing.datadist.kafka.consumer.service.legalentity;
+package com.ing.datadist.kafka.consumer.service.le;
 
 
 import com.ing.datadist.api.utils.DataDistributionConstants;
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/organisationunit/OrganisationHierarchyConsumerService.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/ops/OrganisationHierarchyConsumerService.java
similarity index 98%
rename from DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/organisationunit/OrganisationHierarchyConsumerService.java
rename to DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/ops/OrganisationHierarchyConsumerService.java
index 3c42633..34a7560 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/organisationunit/OrganisationHierarchyConsumerService.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/ops/OrganisationHierarchyConsumerService.java
@@ -1,4 +1,4 @@
-package com.ing.datadist.kafka.consumer.service.organisationunit;
+package com.ing.datadist.kafka.consumer.service.ops;
 
 
 import com.ing.datadist.kafka.util.NotificationProcessingUtil;
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/organisationunit/OrganisationUnitConsumerService.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/ops/OrganisationUnitConsumerService.java
similarity index 98%
rename from DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/organisationunit/OrganisationUnitConsumerService.java
rename to DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/ops/OrganisationUnitConsumerService.java
index 806a053..e16b40a 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/organisationunit/OrganisationUnitConsumerService.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/ops/OrganisationUnitConsumerService.java
@@ -1,4 +1,4 @@
-package com.ing.datadist.kafka.consumer.service.organisationunit;
+package com.ing.datadist.kafka.consumer.service.ops;
 
 
 import com.ing.datadist.kafka.util.NotificationProcessingUtil;
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/organisationunit/OrganisationUnitNameConsumerService.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/ops/OrganisationUnitNameConsumerService.java
similarity index 98%
rename from DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/organisationunit/OrganisationUnitNameConsumerService.java
rename to DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/ops/OrganisationUnitNameConsumerService.java
index 021541b..86239c3 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/organisationunit/OrganisationUnitNameConsumerService.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/kafka/consumer/service/ops/OrganisationUnitNameConsumerService.java
@@ -1,4 +1,4 @@
-package com.ing.datadist.kafka.consumer.service.organisationunit;
+package com.ing.datadist.kafka.consumer.service.ops;
 
 
 import com.ing.datadist.kafka.util.NotificationProcessingUtil;
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/domain/IncapableDomainWrapper.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/domain/IncapableDomainWrapper.java
deleted file mode 100644
index 520ef5d..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/domain/IncapableDomainWrapper.java
+++ /dev/null
@@ -1,34 +0,0 @@
-package com.ing.datadist.kafka.domain;
-
-import lombok.Data;
-
-@Data
-public class IncapableDomainWrapper {
-
-
-    private String ddUuid;
-    private String eventId;
-
-    private Individual individual;
-    private IndividualName individualName;
-    private Occupation occupation;
-    private Marking marking;
-
-
-}
-
-
-
-
-
-
-
-
-
-
-
-
-
-
-
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/domain/Individual.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/domain/Individual.java
deleted file mode 100644
index a9ae577..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/domain/Individual.java
+++ /dev/null
@@ -1,37 +0,0 @@
-package com.ing.datadist.kafka.domain;
-
-import lombok.Data;
-
-@Data
-public class Individual {
-
-    private String ddUuid;
-    private String eventId;
-    private String involvedPartyIdentifier;
-
-    private String gender;
-    private String lastName;
-    private String firstName;
-
-    private String dateOfBirth;
-    private String cityOfBirth;
-    private String countryOfBirth;
-
-    private String dateOfDeathOnePam;
-    private String dateOfDeathDi;
-
-    private PostalAddress postalAddressOnePam;
-    private PostalAddress postalAddressDi;
-
-    private String countryOfResidenceOnePam;
-    private String countryOfResidenceDi;
-
-    private String dataSource;
-    private String individualLifeCycleStatusType;
-    private String effectiveDate;
-    private String endDate;
-}
-
-
-
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/domain/IndividualName.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/domain/IndividualName.java
deleted file mode 100644
index 596bc88..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/domain/IndividualName.java
+++ /dev/null
@@ -1,62 +0,0 @@
-package com.ing.datadist.kafka.domain;
-
-import lombok.Data;
-
-@Data
-public class IndividualName {
-
-    private String ddUuid;
-    private String eventId;
-    private String involvedPartyIdentifier;
-
-    private String individualNameType;
-    private String lastName;
-    private String lastNamePrefix;
-    private String lastNamePrefixDesc;
-    private String givenName;
-    private String firstName1;
-    private String firstName2;
-    private String firstName3;
-    private String firstName4;
-    private String nameInitials;
-    private String nickName;
-    private String secondLastName;
-    private String birthLastName;
-    private String partnerLastName;
-    private String partnerLastNamePrefix;
-    private String partnerLastNamePrefixDesc;
-    private String nameSuffix;
-    private String salutation;
-    private String title1;
-    private String title2;
-    private String title3;
-
-    private String dataSource;
-    private String effectiveDate;
-    private String endDate;
-
-    public String getFullName() {
-        StringBuilder fullName = new StringBuilder();
-
-        if (salutation != null && !salutation.isEmpty()) {
-            fullName.append(salutation).append(" ");
-        }
-        if (firstName1 != null && !firstName1.isEmpty()) {
-            fullName.append(firstName1).append(" ");
-        } else if (givenName != null && !givenName.isEmpty()) {
-            fullName.append(givenName).append(" ");
-        }
-        if (lastNamePrefix != null && !lastNamePrefix.isEmpty()) {
-            fullName.append(lastNamePrefix).append(" ");
-        }
-        if (lastName != null && !lastName.isEmpty()) {
-            fullName.append(lastName);
-        }
-        if (nameSuffix != null && !nameSuffix.isEmpty()) {
-            fullName.append(" ").append(nameSuffix);
-        }
-
-        return fullName.toString().trim();
-    }
-}
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/domain/Marking.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/domain/Marking.java
deleted file mode 100644
index 4287356..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/domain/Marking.java
+++ /dev/null
@@ -1,31 +0,0 @@
-package com.ing.datadist.kafka.domain;
-
-import lombok.Data;
-
-@Data
-public class Marking {
-
-    private String ddUuid;
-    private String eventId;
-    private String involvedPartyIdentifier;
-
-    private String category;
-    private String type;
-
-    private String objectCodeOnePam;
-    private String objectCodeDi;
-
-    private String incapabilityEndDateOnePam;
-    private String incapabilityEndDateDi;
-
-    private String incapabilityEffectiveDateOnePam;
-    private String incapabilityEffectiveDateDi;
-
-    private String dataSource;
-    private String effectiveDate;
-    private String endDate;
-}
-
-
-
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/domain/Occupation.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/domain/Occupation.java
deleted file mode 100644
index 51053c8..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/domain/Occupation.java
+++ /dev/null
@@ -1,33 +0,0 @@
-package com.ing.datadist.kafka.domain;
-
-import lombok.Data;
-
-@Data
-public class Occupation {
-
-    private String ddUuid;
-    private String eventId;
-    private String involvedPartyIdentifier;
-
-    private String occupationCodeOnePam;
-    private String occupationRank;
-    private String occupationCodeDi;
-
-    private String classificationIssuerType;
-    private String classificationCode;
-    private String rank;
-    private String employerName;
-
-    private String industryClassificationIssuerType;
-    private String industryClassificationCode;
-
-    private String responsibilityEndDateOnePam;
-    private String responsibilityEndDateDi;
-
-    private String effectiveDate;
-    private String endDate;
-}
-
-
-
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/util/EventTransactionType.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/util/EventTransactionType.java
index 46a1492..40f6459 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/util/EventTransactionType.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/kafka/util/EventTransactionType.java
@@ -57,47 +57,11 @@ public enum EventTransactionType {
     CREATE_INCAPABLE_OCCUPATION("addPersonOccupation"),
     UPDATE_INCAPABLE_OCCUPATION("updatePersonOccupation"),
     CREATE_INCAPABLE_IP_MARKING("addPartyAlert"),
-    UPDATE_INCAPABLE_IP_MARKING("updatePartyAlert");
+    UPDATE_INCAPABLE_IP_MARKING("updatePartyAlert")
+    ;
 
-    // Transaction Type Prefix Constants
-    public static final String ADD_PREFIX = "add";
-    public static final String UPDATE_PREFIX = "update";
-    public static final String DELETE_PREFIX = "delete";
 
     private final String label;
 
-    /**
-     * Checks if the given transaction type represents an ADD (create) operation.
-     * Determines by checking if the label starts with "add" (case-insensitive).
-     *
-     * @param transactionType the transaction type string from Kafka message
-     * @return true if it's an ADD operation, false otherwise
-     */
-    public static boolean isAddOperation(String transactionType) {
-        return transactionType != null && transactionType.toLowerCase().startsWith(ADD_PREFIX);
-    }
 
-    /**
-     * Checks if the given transaction type represents an UPDATE (modify) operation.
-     * Determines by checking if the label starts with "update" (case-insensitive).
-     *
-     * @param transactionType the transaction type string from Kafka message
-     * @return true if it's an UPDATE operation, false otherwise
-     */
-    public static boolean isUpdateOperation(String transactionType) {
-        return transactionType != null && transactionType.toLowerCase().startsWith(UPDATE_PREFIX);
-    }
-
-    /**
-     * Checks if the given transaction type represents a DELETE (remove) operation.
-     * Determines by checking if the label starts with "delete" (case-insensitive).
-     *
-     * @param transactionType the transaction type string from Kafka message
-     * @return true if it's a DELETE operation, false otherwise
-     */
-    public static boolean isDeleteOperation(String transactionType) {
-        return transactionType != null && transactionType.toLowerCase().startsWith(DELETE_PREFIX);
-    }
 }
-
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/ValidatorFactory.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/ValidatorFactory.java
index 2b7d412..8f57e00 100644
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/ValidatorFactory.java
+++ b/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/ValidatorFactory.java
@@ -1,6 +1,5 @@
 package com.ing.datadist.kafka.validator;
 
-import com.ing.datadist.dao.IncapablesAccountingDAO;
 import com.ing.datadist.dao.OrgChildAccountingDAO;
 import com.ing.datadist.dao.OrgParentAccountingDAO;
 import com.ing.datadist.dao.OrgUnitAccountingDAO;
@@ -11,7 +10,6 @@ import com.ing.datadist.kafka.validator.ops.OrganisationUnitNameValidator;
 import com.ing.datadist.kafka.validator.ops.OrganisationUnitValidator;
 import com.ing.datadist.kafka.validator.ops.PostalAddressValidator;
 import com.ing.datadist.kafka.validator.org.*;
-import com.ing.datadist.kafka.validator.incapable.*;
 import lombok.AllArgsConstructor;
 import lombok.extern.slf4j.Slf4j;
 import org.springframework.stereotype.Component;
@@ -25,7 +23,6 @@ public class ValidatorFactory {
     private final OrgUnitAccountingDAO orgUnitAccountingDAO;
     private final OrgParentAccountingDAO orgParentAccountingDAO;
     private final OrgChildAccountingDAO orgChildAccountingDAO;
-    private final IncapablesAccountingDAO incapablesAccountingDAO;
 
     public Validator<?> getValidator(NotificationType notificationType) {
         if (notificationType == null) return null;
@@ -51,25 +48,6 @@ public class ValidatorFactory {
                     new com.ing.datadist.kafka.validator.org.DigitalAddressValidator(orgParentAccountingDAO);
 
 
-            case INCAPABLE_INDIVIDUAL ->
-                    new com.ing.datadist.kafka.validator.incapable.IndividualValidator(incapablesAccountingDAO);
-            case INCAPABLE_INDIVIDUAL_NAME ->
-                    new com.ing.datadist.kafka.validator.incapable.IndividualNameValidator(incapablesAccountingDAO);
-            case INCAPABLE_INDIVIDUAL_POSTAL_ADDRESS ->
-                    new com.ing.datadist.kafka.validator.incapable.PostalAddressValidator(incapablesAccountingDAO);
-
-            case INCAPABLE_ADMINISTRATOR ->
-                    new com.ing.datadist.kafka.validator.incapable.AdministratorValidator(incapablesAccountingDAO);
-            case INCAPABLE_ADMINISTRATOR_NAME ->
-                    new com.ing.datadist.kafka.validator.incapable.AdministratorNameValidator(incapablesAccountingDAO);
-            case INCAPABLE_ADMINISTRATOR_POSTAL_ADDRESS ->
-                    new com.ing.datadist.kafka.validator.incapable.AdministratorPostalAddressValidator(incapablesAccountingDAO);
-            case INCAPABLE_ADMINISTRATOR_OCCUPATION ->
-                    new com.ing.datadist.kafka.validator.incapable.OccupationValidator(incapablesAccountingDAO);
-            case INCAPABLE_ADMINISTRATOR_IP_MARKING ->
-                    new com.ing.datadist.kafka.validator.incapable.MarkingValidator(incapablesAccountingDAO);
-
-
             default -> null;
         };
     }
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/AdministratorNameValidator.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/AdministratorNameValidator.java
deleted file mode 100644
index 5a4ac84..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/AdministratorNameValidator.java
+++ /dev/null
@@ -1,64 +0,0 @@
-package com.ing.datadist.kafka.validator.incapable;
-
-import com.ing.datadist.dao.IncapablesAccountingDAO;
-import com.ing.datadist.kafka.domain.IndividualName;
-import com.ing.datadist.kafka.util.ComparisonResult;
-import com.ing.datadist.kafka.validator.AbstractValidator;
-import com.ing.datadist.kafka.validator.Validator;
-import lombok.AllArgsConstructor;
-import lombok.extern.slf4j.Slf4j;
-
-import java.util.ArrayList;
-import java.util.List;
-import java.util.Objects;
-
-@Slf4j
-@AllArgsConstructor
-public class AdministratorNameValidator extends AbstractValidator implements Validator<com.ing.postmen.onepam.avro.model.individual.name.IndividualName> {
-
-    private final IncapablesAccountingDAO incapablesAccountingDAO;
-
-    @Override
-    public ComparisonResult validate(com.ing.postmen.onepam.avro.model.individual.name.IndividualName kafkaObject, String involvedPartyIdentifier) {
-        log.info("Starting Administrator Name validation for involvedPartyIdentifier={}", involvedPartyIdentifier);
-
-        IndividualName dbObject = incapablesAccountingDAO.getAdministratorNameByUUID(involvedPartyIdentifier);
-
-        if (dbObject == null) {
-            log.warn("No administrator name found in database for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        } else {
-            log.debug("Retrieved administrator name from database for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        }
-
-        List<String> notEqualFields = new ArrayList<>();
-        int matchCount = 0;
-        int propCount = 0;
-
-        if (dbObject != null && dbObject.getLastName() != null) {
-            if (Objects.equals(kafkaObject.getLastName(), dbObject.getLastName())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: lastName - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getLastName(), kafkaObject.getLastName(), involvedPartyIdentifier);
-                notEqualFields.add("lastName");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getFirstName1() != null) {
-            if (Objects.equals(kafkaObject.getFirstName1(), dbObject.getFirstName1())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: firstName1 - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getFirstName1(), kafkaObject.getFirstName1(), involvedPartyIdentifier);
-                notEqualFields.add("firstName1");
-            }
-            propCount++;
-        }
-
-        log.info("Administrator Name validation completed for involvedPartyIdentifier={}, matchCount={}, propCount={}",
-                involvedPartyIdentifier, matchCount, propCount);
-        return buildComparisonResult(matchCount, propCount, notEqualFields);
-    }
-}
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/AdministratorPostalAddressValidator.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/AdministratorPostalAddressValidator.java
deleted file mode 100644
index 2283d1c..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/AdministratorPostalAddressValidator.java
+++ /dev/null
@@ -1,108 +0,0 @@
-package com.ing.datadist.kafka.validator.incapable;
-
-import com.ing.datadist.dao.IncapablesAccountingDAO;
-import com.ing.datadist.kafka.domain.PostalAddress;
-import com.ing.datadist.kafka.util.ComparisonResult;
-import com.ing.datadist.kafka.validator.AbstractValidator;
-import com.ing.datadist.kafka.validator.Validator;
-import lombok.AllArgsConstructor;
-import lombok.extern.slf4j.Slf4j;
-
-import java.util.ArrayList;
-import java.util.List;
-import java.util.Objects;
-
-@Slf4j
-@AllArgsConstructor
-public class AdministratorPostalAddressValidator extends AbstractValidator implements Validator<com.ing.postmen.onepam.avro.model.postaladdress.PostalAddress> {
-
-    private final IncapablesAccountingDAO incapablesAccountingDAO;
-
-    @Override
-    public ComparisonResult validate(com.ing.postmen.onepam.avro.model.postaladdress.PostalAddress kafkaObject, String involvedPartyIdentifier) {
-        log.info("Starting Administrator PostalAddress validation for involvedPartyIdentifier={}", involvedPartyIdentifier);
-
-        PostalAddress dbObject = incapablesAccountingDAO.getAdministratorAddressByUUID(involvedPartyIdentifier);
-
-        if (dbObject == null) {
-            log.warn("No administrator postal address found in database for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        } else {
-            log.debug("Retrieved administrator postal address from database for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        }
-
-        List<String> notEqualFields = new ArrayList<>();
-        int matchCount = 0;
-        int propCount = 0;
-
-        if (dbObject != null && dbObject.getStreetName() != null) {
-            if (Objects.equals(kafkaObject.getStreetName(), dbObject.getStreetName())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: streetName - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getStreetName(), kafkaObject.getStreetName(), involvedPartyIdentifier);
-                notEqualFields.add("streetName");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getHouseNumber() != null) {
-            if (Objects.equals(kafkaObject.getHouseNumber(), dbObject.getHouseNumber())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: houseNumber - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getHouseNumber(), kafkaObject.getHouseNumber(), involvedPartyIdentifier);
-                notEqualFields.add("houseNumber");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getHouseNumberAddition() != null) {
-            if (Objects.equals(kafkaObject.getHouseNumberAddition(), dbObject.getHouseNumberAddition())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: houseNumberAddition - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getHouseNumberAddition(), kafkaObject.getHouseNumberAddition(), involvedPartyIdentifier);
-                notEqualFields.add("houseNumberAddition");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getPostalCode() != null) {
-            if (Objects.equals(kafkaObject.getPostalCode(), dbObject.getPostalCode())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: postalCode - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getPostalCode(), kafkaObject.getPostalCode(), involvedPartyIdentifier);
-                notEqualFields.add("postalCode");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getCityName() != null) {
-            if (Objects.equals(kafkaObject.getCityName(), dbObject.getCityName())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: cityName - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getCityName(), kafkaObject.getCityName(), involvedPartyIdentifier);
-                notEqualFields.add("cityName");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getCountryCode() != null) {
-            if (Objects.equals(kafkaObject.getCountryCode(), dbObject.getCountryCode())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: countryCode - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getCountryCode(), kafkaObject.getCountryCode(), involvedPartyIdentifier);
-                notEqualFields.add("countryCode");
-            }
-            propCount++;
-        }
-
-        log.info("Administrator PostalAddress validation completed for involvedPartyIdentifier={}, matchCount={}, propCount={}",
-                involvedPartyIdentifier, matchCount, propCount);
-        return buildComparisonResult(matchCount, propCount, notEqualFields);
-    }
-}
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/AdministratorValidator.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/AdministratorValidator.java
deleted file mode 100644
index 3c4c2a3..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/AdministratorValidator.java
+++ /dev/null
@@ -1,107 +0,0 @@
-package com.ing.datadist.kafka.validator.incapable;
-
-import com.ing.datadist.dao.IncapablesAccountingDAO;
-import com.ing.datadist.kafka.domain.Individual;
-import com.ing.datadist.kafka.util.ComparisonResult;
-import com.ing.datadist.kafka.validator.AbstractValidator;
-import com.ing.datadist.kafka.validator.Validator;
-import lombok.AllArgsConstructor;
-import lombok.extern.slf4j.Slf4j;
-
-import java.util.ArrayList;
-import java.util.List;
-import java.util.Objects;
-
-@Slf4j
-@AllArgsConstructor
-public class AdministratorValidator extends AbstractValidator implements Validator<com.ing.postmen.onepam.avro.model.individual.Individual> {
-
-    private final IncapablesAccountingDAO incapablesAccountingDAO;
-
-    @Override
-    public ComparisonResult validate(com.ing.postmen.onepam.avro.model.individual.Individual kafkaObject, String involvedPartyIdentifier) {
-        log.info("Starting Administrator Individual validation for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        Individual dbObject = incapablesAccountingDAO.getIndividualByUUID(involvedPartyIdentifier);
-
-        if (dbObject == null) {
-            log.warn("No administrator found in database for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        } else {
-            log.debug("Retrieved administrator from database for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        }
-
-        List<String> notEqualFields = new ArrayList<>();
-        int matchCount = 0;
-        int propCount = 0;
-
-        if (dbObject != null && dbObject.getGender() != null) {
-            if (Objects.equals(kafkaObject.getGender(), dbObject.getGender())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: gender - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getGender(), kafkaObject.getGender(), involvedPartyIdentifier);
-                notEqualFields.add("gender");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getDateOfBirth() != null) {
-            if (Objects.equals(kafkaObject.getDateOfBirth(), dbObject.getDateOfBirth())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: dateOfBirth - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getDateOfBirth(), kafkaObject.getDateOfBirth(), involvedPartyIdentifier);
-                notEqualFields.add("dateOfBirth");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getCityOfBirth() != null) {
-            if (Objects.equals(kafkaObject.getCityOfBirth(), dbObject.getCityOfBirth())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: cityOfBirth - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getCityOfBirth(), kafkaObject.getCityOfBirth(), involvedPartyIdentifier);
-                notEqualFields.add("cityOfBirth");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getCountryOfBirth() != null) {
-            if (Objects.equals(kafkaObject.getCountryOfBirth(), dbObject.getCountryOfBirth())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: countryOfBirth - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getCountryOfBirth(), kafkaObject.getCountryOfBirth(), involvedPartyIdentifier);
-                notEqualFields.add("countryOfBirth");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getDateOfDeathDi() != null) {
-            if (Objects.equals(kafkaObject.getDateOfDeath(), dbObject.getDateOfDeathDi())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: dateOfDeath - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getDateOfDeathDi(), kafkaObject.getDateOfDeath(), involvedPartyIdentifier);
-                notEqualFields.add("dateOfDeath");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getCountryOfResidenceDi() != null) {
-            if (Objects.equals(kafkaObject.getCountryOfResidence(), dbObject.getCountryOfResidenceDi())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: countryOfResidence - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getCountryOfResidenceDi(), kafkaObject.getCountryOfResidence(), involvedPartyIdentifier);
-                notEqualFields.add("countryOfResidence");
-            }
-            propCount++;
-        }
-
-        log.info("Administrator Individual validation completed for involvedPartyIdentifier={}, matchCount={}, propCount={}",
-                involvedPartyIdentifier, matchCount, propCount);
-        return buildComparisonResult(matchCount, propCount, notEqualFields);
-    }
-}
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/IndividualNameValidator.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/IndividualNameValidator.java
deleted file mode 100644
index f5bd7eb..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/IndividualNameValidator.java
+++ /dev/null
@@ -1,65 +0,0 @@
-package com.ing.datadist.kafka.validator.incapable;
-
-import com.ing.datadist.dao.IncapablesAccountingDAO;
-import com.ing.datadist.kafka.domain.IndividualName;
-import com.ing.datadist.kafka.util.ComparisonResult;
-import com.ing.datadist.kafka.validator.AbstractValidator;
-import com.ing.datadist.kafka.validator.Validator;
-import lombok.AllArgsConstructor;
-import lombok.extern.slf4j.Slf4j;
-
-import java.util.ArrayList;
-import java.util.List;
-import java.util.Objects;
-
-@Slf4j
-@AllArgsConstructor
-public class IndividualNameValidator extends AbstractValidator implements Validator<com.ing.postmen.onepam.avro.model.individual.name.IndividualName> {
-
-    private final IncapablesAccountingDAO incapablesAccountingDAO;
-
-    @Override
-    public ComparisonResult validate(com.ing.postmen.onepam.avro.model.individual.name.IndividualName kafkaObject, String involvedPartyIdentifier) {
-        log.info("Starting Incapable IndividualName validation for involvedPartyIdentifier={}", involvedPartyIdentifier);
-
-        IndividualName dbObject = incapablesAccountingDAO.getIndividualNameByUUID(involvedPartyIdentifier);
-
-        if (dbObject == null) {
-            log.warn("No incapable individual name found in database for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        } else {
-            log.debug("Retrieved incapable individual name from database for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        }
-
-        List<String> notEqualFields = new ArrayList<>();
-        int matchCount = 0;
-        int propCount = 0;
-
-        if (dbObject != null && dbObject.getLastName() != null) {
-            if (Objects.equals(kafkaObject.getLastName(), dbObject.getLastName())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: lastName - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getLastName(), kafkaObject.getLastName(), involvedPartyIdentifier);
-                notEqualFields.add("lastName");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getFirstName1() != null) {
-            if (Objects.equals(kafkaObject.getFirstName1(), dbObject.getFirstName1())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: firstName1 - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getFirstName1(), kafkaObject.getFirstName1(), involvedPartyIdentifier);
-                notEqualFields.add("firstName1");
-            }
-            propCount++;
-        }
-
-        log.info("Incapable IndividualName validation completed for involvedPartyIdentifier={}, matchCount={}, propCount={}",
-                involvedPartyIdentifier, matchCount, propCount);
-        return buildComparisonResult(matchCount, propCount, notEqualFields);
-    }
-}
-
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/IndividualValidator.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/IndividualValidator.java
deleted file mode 100644
index f3d3db4..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/IndividualValidator.java
+++ /dev/null
@@ -1,109 +0,0 @@
-package com.ing.datadist.kafka.validator.incapable;
-
-import com.ing.datadist.dao.IncapablesAccountingDAO;
-import com.ing.datadist.kafka.domain.Individual;
-import com.ing.datadist.kafka.util.ComparisonResult;
-import com.ing.datadist.kafka.validator.AbstractValidator;
-import com.ing.datadist.kafka.validator.Validator;
-import lombok.AllArgsConstructor;
-import lombok.extern.slf4j.Slf4j;
-
-import java.util.ArrayList;
-import java.util.List;
-import java.util.Objects;
-
-@Slf4j
-@AllArgsConstructor
-public class IndividualValidator extends AbstractValidator implements Validator<com.ing.postmen.onepam.avro.model.individual.Individual> {
-
-    private final IncapablesAccountingDAO incapablesAccountingDAO;
-
-    @Override
-    public ComparisonResult validate(com.ing.postmen.onepam.avro.model.individual.Individual kafkaObject, String involvedPartyIdentifier) {
-        log.info("Starting Incapable Individual validation for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        Individual dbObject = incapablesAccountingDAO.getIndividualByUUID(involvedPartyIdentifier);
-
-        if (dbObject == null) {
-            log.warn("No incapable individual found in database for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        } else {
-            log.debug("Retrieved incapable individual from database for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        }
-
-        List<String> notEqualFields = new ArrayList<>();
-        int matchCount = 0;
-        int propCount = 0;
-
-        if (dbObject != null && dbObject.getGender() != null) {
-            if (Objects.equals(kafkaObject.getGender(), dbObject.getGender())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: gender - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getGender(), kafkaObject.getGender(), involvedPartyIdentifier);
-                notEqualFields.add("gender");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getDateOfBirth() != null) {
-            if (Objects.equals(kafkaObject.getDateOfBirth(), dbObject.getDateOfBirth())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: dateOfBirth - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getDateOfBirth(), kafkaObject.getDateOfBirth(), involvedPartyIdentifier);
-                notEqualFields.add("dateOfBirth");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getCityOfBirth() != null) {
-            if (Objects.equals(kafkaObject.getCityOfBirth(), dbObject.getCityOfBirth())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: cityOfBirth - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getCityOfBirth(), kafkaObject.getCityOfBirth(), involvedPartyIdentifier);
-                notEqualFields.add("cityOfBirth");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getCountryOfBirth() != null) {
-            if (Objects.equals(kafkaObject.getCountryOfBirth(), dbObject.getCountryOfBirth())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: countryOfBirth - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getCountryOfBirth(), kafkaObject.getCountryOfBirth(), involvedPartyIdentifier);
-                notEqualFields.add("countryOfBirth");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getDateOfDeathDi() != null) {
-            if (Objects.equals(kafkaObject.getDateOfDeath(), dbObject.getDateOfDeathDi())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: dateOfDeath - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getDateOfDeathDi(), kafkaObject.getDateOfDeath(), involvedPartyIdentifier);
-                notEqualFields.add("dateOfDeath");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getCountryOfResidenceDi() != null) {
-            if (Objects.equals(kafkaObject.getCountryOfResidence(), dbObject.getCountryOfResidenceDi())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: countryOfResidence - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getCountryOfResidenceDi(), kafkaObject.getCountryOfResidence(), involvedPartyIdentifier);
-                notEqualFields.add("countryOfResidence");
-            }
-            propCount++;
-        }
-
-        log.info("Incapable Individual validation completed for involvedPartyIdentifier={}, matchCount={}, propCount={}",
-                involvedPartyIdentifier, matchCount, propCount);
-        return buildComparisonResult(matchCount, propCount, notEqualFields);
-    }
-}
-
-
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/MarkingValidator.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/MarkingValidator.java
deleted file mode 100644
index 6378171..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/MarkingValidator.java
+++ /dev/null
@@ -1,78 +0,0 @@
-package com.ing.datadist.kafka.validator.incapable;
-
-import com.ing.datadist.dao.IncapablesAccountingDAO;
-import com.ing.datadist.kafka.domain.Marking;
-import com.ing.datadist.kafka.util.ComparisonResult;
-import com.ing.datadist.kafka.validator.AbstractValidator;
-import com.ing.datadist.kafka.validator.Validator;
-import com.ing.postmen.onepam.avro.model.marking.InvolvedPartyMarking;
-import lombok.AllArgsConstructor;
-import lombok.extern.slf4j.Slf4j;
-
-import java.util.ArrayList;
-import java.util.List;
-import java.util.Objects;
-
-@Slf4j
-@AllArgsConstructor
-public class MarkingValidator extends AbstractValidator implements Validator<InvolvedPartyMarking> {
-
-    private final IncapablesAccountingDAO incapablesAccountingDAO;
-
-    @Override
-    public ComparisonResult validate(InvolvedPartyMarking kafkaObject, String involvedPartyIdentifier) {
-        log.info("Starting Administrator Marking validation for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        Marking dbObject = incapablesAccountingDAO.getMarkingByUUID(involvedPartyIdentifier);
-
-        if (dbObject == null) {
-            log.warn("No marking found in database for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        } else {
-            log.debug("Retrieved marking from database for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        }
-
-        List<String> notEqualFields = new ArrayList<>();
-        int matchCount = 0;
-        int propCount = 0;
-
-        if (dbObject != null && dbObject.getType() != null) {
-            if (Objects.equals(kafkaObject.getType(), dbObject.getType())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: type - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getType(), kafkaObject.getType(), involvedPartyIdentifier);
-                notEqualFields.add("type");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getEffectiveDate() != null) {
-            if (Objects.equals(kafkaObject.getEffectiveDate(), dbObject.getEffectiveDate())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: effectiveDate - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getEffectiveDate(), kafkaObject.getEffectiveDate(), involvedPartyIdentifier);
-                notEqualFields.add("effectiveDate");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getEndDate() != null) {
-            if (Objects.equals(kafkaObject.getEndDate(), dbObject.getEndDate())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: endDate - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getEndDate(), kafkaObject.getEndDate(), involvedPartyIdentifier);
-                notEqualFields.add("endDate");
-            }
-            propCount++;
-        }
-
-        log.info("Administrator Marking validation completed for involvedPartyIdentifier={}, matchCount={}, propCount={}",
-                involvedPartyIdentifier, matchCount, propCount);
-        return buildComparisonResult(matchCount, propCount, notEqualFields);
-    }
-}
-
-
-
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/OccupationValidator.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/OccupationValidator.java
deleted file mode 100644
index 7c6688a..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/OccupationValidator.java
+++ /dev/null
@@ -1,75 +0,0 @@
-package com.ing.datadist.kafka.validator.incapable;
-
-import com.ing.datadist.dao.IncapablesAccountingDAO;
-import com.ing.datadist.kafka.domain.Occupation;
-import com.ing.datadist.kafka.util.ComparisonResult;
-import com.ing.datadist.kafka.validator.AbstractValidator;
-import com.ing.datadist.kafka.validator.Validator;
-import lombok.AllArgsConstructor;
-import lombok.extern.slf4j.Slf4j;
-
-import java.util.ArrayList;
-import java.util.List;
-import java.util.Objects;
-
-@Slf4j
-@AllArgsConstructor
-public class OccupationValidator extends AbstractValidator implements Validator<com.ing.postmen.onepam.avro.model.individual.occupation.Occupation> {
-
-    private final IncapablesAccountingDAO incapablesAccountingDAO;
-
-    @Override
-    public ComparisonResult validate(com.ing.postmen.onepam.avro.model.individual.occupation.Occupation kafkaObject, String involvedPartyIdentifier) {
-        log.info("Starting Administrator Occupation validation for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        Occupation dbObject = incapablesAccountingDAO.getOccupationByUUID(involvedPartyIdentifier);
-
-        if (dbObject == null) {
-            log.warn("No occupation found in database for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        } else {
-            log.debug("Retrieved occupation from database for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        }
-
-        List<String> notEqualFields = new ArrayList<>();
-        int matchCount = 0;
-        int propCount = 0;
-
-        if (dbObject != null && dbObject.getClassificationCode() != null) {
-            if (Objects.equals(kafkaObject.getClassificationCode(), dbObject.getClassificationCode())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: classificationCode - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getClassificationCode(), kafkaObject.getClassificationCode(), involvedPartyIdentifier);
-                notEqualFields.add("classificationCode");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getRank() != null) {
-            if (Objects.equals(kafkaObject.getRank(), dbObject.getRank())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: rank - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getRank(), kafkaObject.getRank(), involvedPartyIdentifier);
-                notEqualFields.add("rank");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getEndDate() != null) {
-            if (Objects.equals(kafkaObject.getEndDate(), dbObject.getEndDate())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: endDate - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getEndDate(), kafkaObject.getEndDate(), involvedPartyIdentifier);
-                notEqualFields.add("endDate");
-            }
-            propCount++;
-        }
-
-        log.info("Administrator Occupation validation completed for involvedPartyIdentifier={}, matchCount={}, propCount={}",
-                involvedPartyIdentifier, matchCount, propCount);
-        return buildComparisonResult(matchCount, propCount, notEqualFields);
-    }
-}
-
-
diff --git a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/PostalAddressValidator.java b/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/PostalAddressValidator.java
deleted file mode 100644
index f8d1525..0000000
--- a/DataDistribution/src/main/java/com/ing/datadist/kafka/validator/incapable/PostalAddressValidator.java
+++ /dev/null
@@ -1,109 +0,0 @@
-package com.ing.datadist.kafka.validator.incapable;
-
-import com.ing.datadist.dao.IncapablesAccountingDAO;
-import com.ing.datadist.kafka.domain.PostalAddress;
-import com.ing.datadist.kafka.util.ComparisonResult;
-import com.ing.datadist.kafka.validator.AbstractValidator;
-import com.ing.datadist.kafka.validator.Validator;
-import lombok.AllArgsConstructor;
-import lombok.extern.slf4j.Slf4j;
-
-import java.util.ArrayList;
-import java.util.List;
-import java.util.Objects;
-
-@Slf4j
-@AllArgsConstructor
-public class PostalAddressValidator extends AbstractValidator implements Validator<com.ing.postmen.onepam.avro.model.postaladdress.PostalAddress> {
-
-    private final IncapablesAccountingDAO incapablesAccountingDAO;
-
-    @Override
-    public ComparisonResult validate(com.ing.postmen.onepam.avro.model.postaladdress.PostalAddress kafkaObject, String involvedPartyIdentifier) {
-        log.info("Starting Incapable PostalAddress validation for involvedPartyIdentifier={}", involvedPartyIdentifier);
-
-        PostalAddress dbObject = incapablesAccountingDAO.getIncapableAddressByUUID(involvedPartyIdentifier);
-
-        if (dbObject == null) {
-            log.warn("No incapable postal address found in database for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        } else {
-            log.debug("Retrieved incapable postal address from database for involvedPartyIdentifier={}", involvedPartyIdentifier);
-        }
-
-        List<String> notEqualFields = new ArrayList<>();
-        int matchCount = 0;
-        int propCount = 0;
-
-        if (dbObject != null && dbObject.getStreetName() != null) {
-            if (Objects.equals(kafkaObject.getStreetName(), dbObject.getStreetName())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: streetName - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getStreetName(), kafkaObject.getStreetName(), involvedPartyIdentifier);
-                notEqualFields.add("streetName");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getHouseNumber() != null) {
-            if (Objects.equals(kafkaObject.getHouseNumber(), dbObject.getHouseNumber())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: houseNumber - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getHouseNumber(), kafkaObject.getHouseNumber(), involvedPartyIdentifier);
-                notEqualFields.add("houseNumber");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getHouseNumberAddition() != null) {
-            if (Objects.equals(kafkaObject.getHouseNumberAddition(), dbObject.getHouseNumberAddition())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: houseNumberAddition - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getHouseNumberAddition(), kafkaObject.getHouseNumberAddition(), involvedPartyIdentifier);
-                notEqualFields.add("houseNumberAddition");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getPostalCode() != null) {
-            if (Objects.equals(kafkaObject.getPostalCode(), dbObject.getPostalCode())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: postalCode - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getPostalCode(), kafkaObject.getPostalCode(), involvedPartyIdentifier);
-                notEqualFields.add("postalCode");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getCityName() != null) {
-            if (Objects.equals(kafkaObject.getCityName(), dbObject.getCityName())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: cityName - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getCityName(), kafkaObject.getCityName(), involvedPartyIdentifier);
-                notEqualFields.add("cityName");
-            }
-            propCount++;
-        }
-
-        if (dbObject != null && dbObject.getCountryCode() != null) {
-            if (Objects.equals(kafkaObject.getCountryCode(), dbObject.getCountryCode())) {
-                matchCount++;
-            } else {
-                log.debug("Mismatch: countryCode - DB: {}, Kafka: {}, involvedPartyIdentifier={}",
-                        dbObject.getCountryCode(), kafkaObject.getCountryCode(), involvedPartyIdentifier);
-                notEqualFields.add("countryCode");
-            }
-            propCount++;
-        }
-
-        log.info("Incapable PostalAddress validation completed for involvedPartyIdentifier={}, matchCount={}, propCount={}",
-                involvedPartyIdentifier, matchCount, propCount);
-        return buildComparisonResult(matchCount, propCount, notEqualFields);
-    }
-}
-
-
diff --git a/DataDistribution/src/main/resources/COFACE_LE_DEL.csv b/DataDistribution/src/main/resources/COFACE_LE_DEL.csv
deleted file mode 100644
index 7573211..0000000
--- a/DataDistribution/src/main/resources/COFACE_LE_DEL.csv
+++ /dev/null
@@ -1,8 +0,0 @@
-"LE_ExterN"alIdeN"tifier","LE_DELETION"FLAG
-"1001841042","Y"
-"211453268","Y"
-"1001000002","N"
-"220976985","Y"
-"1001000019","Y"
-"402558908","N"
-6
diff --git a/DataDistribution/src/main/resources/CofaceIncapables.csv b/DataDistribution/src/main/resources/CofaceIncapables.csv
index f747683..747d814 100644
--- a/DataDistribution/src/main/resources/CofaceIncapables.csv
+++ b/DataDistribution/src/main/resources/CofaceIncapables.csv
@@ -1,5 +1,89 @@
-"incp_gender","incp_lastname","incp_firstname","incp_streetname","incp_housenumber","incp_housenumberaddition","incp_postalcode","incp_cityname","incp_countryofresidence","incp_dateofbirth","incp_dateofdeath","incp_cityofbirth","incp_countryofbirth","object_code","inab_enddate","inab_effectivedate","adm_occupationcode","adm_lastname","adm_firstname","adm_streetname","adm_housenumber","adm_housenumberaddition","adm_postalcode","adm_cityname","adm_countryofresidence","adm_responsibility_enddate"
-"ML","Hallet","Rimbaud","Rue de Cornillon","44","0021","4020","LIEGE","BE","19980515","","SERAING","BE","1001","","","","Deguel","Fran├â┬ºois","Rue des Vennes","91","","4020","LIEGE","BE",
-"FEML","Lahaye","Jessica","Rue L├â┬⌐on Dubois","279","1","6030","CHARLEROI","BE","19980114","","MONS","BE","1002","","","160","Coudou","Laurence","Boulevard Audent","11","1","6000","CHARLEROI","BE",
-"FEML","Dessy","Laurence","Rue L├â┬⌐o Darton","58","","6030","CHARLEROI","BE","19690716","","DINANT","BE","1002","","","160","Palate","St├â┬⌐phanie","Boulevard Audent","11","3","6000","CHARLEROI","BE",
-3
\ No newline at end of file
+incp_gender,incp_lastname,incp_firstname,incp_streetname,incp_housenumber,incp_housenumberaddition,incp_postalcode,incp_cityname,incp_countryofresidence,incp_dateofbirth,incp_dateofdeath,incp_cityofbirth,incp_countryofbirth,object_code,inab_enddate,inab_effectivedate,adm_occupationcode,adm_lastname,adm_firstname,adm_streetname,adm_housenumber,adm_housenumberaddition,adm_postalcode,adm_cityname,adm_countryofresidence,adm_responsibility_enddate
+ML,Hallet,Rimbaud,Rue de Cornillon,44,0021,4020,LIEGE,BE,19980515,,SERAING,BE,1001,,,,Deguel,Fran├â┬ºois,Rue des Vennes,91,,4020,LIEGE,BE,
+FEML,Lahaye,Jessica,Rue L├â┬⌐on Dubois,279,1,6030,CHARLEROI,BE,19980114,,MONS,BE,1002,,,160,Coudou,Laurence,Boulevard Audent,11,1,6000,CHARLEROI,BE,
+FEML,Dessy,Laurence,Rue L├â┬⌐o Darton,58,,6030,CHARLEROI,BE,19690716,,DINANT,BE,1002,,,160,Palate,St├â┬⌐phanie,Boulevard Audent,11,3,6000,CHARLEROI,BE,
+ML,Pottier,Yves,Rue Walth├â┬¿re Jamar,51,,4430,ANS,BE,19530525,,LIEGE,BE,1001,,,160,Cuyvers,C├â┬⌐dric,Rue des Ecoliers,62-64,,4100,SERAING,BE,
+FEML,De Guissm├â┬⌐,Sandrine,Chemin de la Basse Couture,13B,,7860,LESSINES,BE,19810110,,LESSINES,BE,1001,,,,De Guissm├â┬⌐,Daniel,Chemin de la Basse Couture,13B,,7860,LESSINES,BE,
+FEML,Verlaine,Carmen,Avenue Jean Jaur├â┬¿s,,,6791,AUBANGE,BE,19290926,,ATHUS,BE,1001,,,,Lambert,Mireille,Rue de France,38,,6791,AUBANGE,BE,
+ML,Kobeszko,J├â┬│zef,Rue du Tr├â┬┤ne,67,1,1050,IXELLES,BE,19590318,,Bia├à┬éystok,PL,1002,,,160,Bruck,Val├â┬⌐rie,Rue Defacqz,78,10,1060,SAINT-GILLES,BE,
+ML,Van de Maele,Danny,Kuipersstraat,31,9,8000,BRUGGE,BE,19570415,,NINOVE,BE,1001,,,160,Van Damme,Annelies,Molenstraat,19,,8750,WINGENE,BE,
+FEML,Verhoeven,Christiane,Geerdegem-Schonenberg,146,102,2800,MECHELEN,BE,19520413,,LEUVEN,BE,1001,,,,Van Haezendonck,Michel,Kanadastraat,21,,2860,SINT-KATELIJNE-WAVER,BE,
+,Frankignoul,Nicolas,Boulevard Saucy,31,0031,4020,LIEGE,BE,19841226,,LIEGE,BE,1001,,,160,Kriescher,Pauline,Rue Courtois,16,,4000,LIEGE,BE,
+FEML,Segers,Maria,Hondsbergstraat,28,,1785,MERCHTEM,BE,19390608,,BRUSSEGEM,BE,1001,,,160,Verhaeghe,Eline,Hoeveland,32,,1850,GRIMBERGEN,BE,
+ML,Houtain,Renaud,Le Ch├â┬⌐nia,6,,1460,ITTRE,BE,19850812,,BRUXELLES,BE,1001,,,160,Lanckmans,Laurie,Avenue Louise,391,7,1050,BRUXELLES,BE,
+ML,Deltour,John,Rue Despars,94,,7500,TOURNAI,BE,19930602,,LIEGE,BE,1002,,,160,Huez,Geoffroy,Rue Child├â┬⌐ric,41,,7500,TOURNAI,BE,
+ML,Damoisiaux,Ga├â┬½tan,Rue de la Villette,15,1,6001,CHARLEROI,BE,19940211,,NAMUR,BE,1002,,,160,Coudou,Laurence,Boulevard Audent,11,1,6000,CHARLEROI,BE,
+FEML,Hribersek,Carine,Rue du Chenois,1,,6030,CHARLEROI,BE,19690414,,LOBBES,BE,1002,,,160,Coudou,Laurence,Boulevard Audent,11,1,6000,CHARLEROI,BE,
+FEML,Warrot,Godelieve,Warandestraat,39,11,8790,WAREGEM,BE,19340704,,DEERLIJK,BE,1001,,,160,Cloet,Delphine,Guido Gezellestraat,10,,8790,WAREGEM,BE,
+FEML,Gijpen,Steffi,Steppeke,54,,1981,ZEMST,BE,20040824,,,,1001,,,160,Van Borm,Lydia,Cardijnstraat,35,,1980,ZEMST,BE,
+FEML,Cottyn,Lieze,Elfde-julistraat,221,,8530,HARELBEKE,BE,19870912,,KORTRIJK,BE,1001,,,160,Maes,Nico,Gentstraat,54,,8760,TIELT,BE,
+,Kor├â┬⌐,Alexandre,Rue du Korenbeek,248,0005,1080,MOLENBEEK-SAINT-JEAN,BE,20060719,,BRUXELLES,BE,1001,,,,Ohouman,Edwige,Rue du Korenbeek,248,0005,1080,MOLENBEEK-SAINT-JEAN,BE,
+FEML,Pliez,Marjorie,Avenue du Chili,2,34,6001,CHARLEROI,BE,19780612,,FRAMERIES,BE,1002,,,160,Palate,St├â┬⌐phanie,Boulevard Audent,11,3,6000,CHARLEROI,BE,
+FEML,Dubus,Arlette,Leopoldstraat,26,,8580,AVELGEM,BE,19340124,,SINT-DENIJS,BE,1001,,,160,De Geeter,Stefaan,Beheerstraat,70,,8500,KORTRIJK,BE,
+,Ehbali,Noureddine,Rue du Pont de Wandre,72,0001,4020,LIEGE,BE,19640526,,OUJDA,MA,1001,,,160,Levy,Philippe,Boulevard de la Sauveni├â┬¿re,136A,,4000,LIEGE,BE,
+FEML,Michaux,Sabine,Rue des Chenays,121,,6921,WELLIN,BE,19360703,,NAMUR,BE,1002,,,160,Closson,Benoit,Rue Houchettes,19,1,6920,WELLIN,BE,
+FEML,Delguste,Jocelyne,Chemin des Rocs,51,,7600,PERUWELZ,BE,19470613,,BOUSSU,BE,1002,,,160,Saint Martin,Marceline,Rue de la Vieille Eglise,1A,,6810,CHINY,BE,
+ML,Slootmaekers,Bart,Sint-Truiderstraat,37,3,3700,TONGEREN-BORGLOON,BE,19840703,,BILZEN,BE,1001,,,160,Reard,Frederik,Klinkerstraat,19,,3700,TONGEREN-BORGLOON,BE,
+FEML,Wilkinson,B├â┬⌐atrice Amber Alice,Rue du Gros Buisson,15D,,7040,QUEVY,BE,19470205,,Monterey,US,1002,,,160,Fries,D├â┬⌐borah,Rue Emile Vandervelde,104,,7033,MONS,BE,
+FEML,Hottechamps,Jos├â┬⌐e,Rue Cardinal Mercier,22,,4633,SOUMAGNE,BE,19480417,,LIEGE,BE,1001,,,160,Kelecom,Tanguy,Rue des Ecoliers,7,,4020,LIEGE,BE,
+ML,Thirion,Alan,Rue Ren├â┬⌐ S├â┬⌐r├â┬⌐siat,14,,6840,NEUFCHATEAU,BE,20070603,,LIBRAMONT-CHEVIGNY,BE,1001,,,160,Moniotte,Jean-Fran├â┬ºois,Rue du Serpont,29A,,6800,LIBRAMONT-CHEVIGNY,BE,
+ML,Thill,Andrews,Rue Fran├â┬ºois Sarteel,200,,5060,SAMBREVILLE,BE,20060709,,MARCHE-EN-FAMENNE,BE,1002,,,160,Escarmelle,Colombine,Boulevard Fr├â┬¿re Orban,3,,5000,NAMUR,BE,
+ML,Baeyens,Willy,Daalstraat,37,,9420,ERPE-MERE,BE,19380629,,MERE,BE,1001,,,160,Van Steenberge,Gerda,Hofveldweg,2,,9420,AAIGEM,BE,
+ML,Nachat,Rayane,Avenue Mahatma Gandhi,2,0015,1080,MOLENBEEK-SAINT-JEAN,BE,20070903,,BRUXELLES,BE,1001,,,,Nachat,Ikram,Avenue Mahatma Gandhi,2,0015,1080,MOLENBEEK-SAINT-JEAN,BE,
+FEML,Lacharon,Sophie,Rue Ardoncour,42,,4633,SOUMAGNE,BE,19940510,,,,1002,,,160,Omari,Fatima,Rue de Rotheux,39,,4100,SERAING,BE,
+ML,Tulutulu Ba,Miche,Rue Haute,16,,6791,AUBANGE,BE,20051123,,Helsinki,FI,1001,,,,Ngabidulu Manilundu,Marc,Rue Haute,16,,6791,AUBANGE,BE,
+ML,Kallat,Karim,Schapulierstraat,4,02-1,1800,VILVOORDE,BE,19741030,,VILVOORDE,BE,1002,,,160,Tock,An,Hendrik I-lei,34,,1800,VILVOORDE,BE,
+,Ledoux,Isabelle,rue des Tilleuls,83,,5680,DOISCHE,BE,19700818,,CHARLEROI,BE,1002,,,160,Lejour,Anny,Rue du Rempart,11,,6200,CHATELET,BE,
+ML,Foulon,Richard,Streyestraat,34,,8554,ZWEVEGEM,BE,19350730,,SINT-DENIJS,BE,1001,,,160,De Geeter,Stefaan,Beheerstraat,70,,8500,KORTRIJK,BE,
+FEML,Lors├â┬⌐,Vanessa,Avenue Marius Meur├â┬⌐e,68,021,6001,CHARLEROI,BE,19790521,,CHARLEROI,BE,1002,,,160,Coudou,Laurence,Boulevard Audent,11,1,6000,CHARLEROI,BE,
+FEML,De Guissm├â┬⌐,Sandrine,Chemin de la Basse Couture,13B,,7860,LESSINES,BE,19810110,,LESSINES,BE,1001,,,,Liv├â┬⌐mont,Alix,Chemin de la Basse Couture,13B,,7860,LESSINES,BE,
+FEML,Cornet,Micheline Nelly Ren├â┬⌐e Alfr├â┬¿de Philippine,Rue de Heusy,7,,4800,VERVIERS,BE,19330820,,BRESSOUX,BE,1001,,,160,Halleux,Genevi├â┬¿ve,Rue Laoureux,37,,4800,VERVIERS,BE,
+ML,Parys,Ronald,,,,,,,19600914,,,,1002,,,160,Laforce,Kelly,Peter Benoitstraat,32,,2018,ANTWERPEN,BE,
+ML,Levantis,Ioannis,Rue Aim├â┬⌐ Mignolet,10,1,6061,MONTIGNIES-SUR-SAMBRE,BE,19680810,,MONTIGNIES-SUR-SAMBRE,BE,1001,,,160,Dubuisson,Brigitte,Rue de Viesville,26,,6180,COURCELLES,BE,
+,Nachampassak,Vimala,Haagbeuk,8,,1730,ASSE,BE,19940624,,BRUSSEL,BE,1001,,,160,Fayt,Annelies,Bloklaan,44,,1730,ASSE,BE,
+ML,De Laet,Jelle,Andreas Vesaliuslaan,39,,2980,ZOERSEL,BE,19831022,,SCHOTEN,BE,1002,,,160,Laforce,Kelly,Peter Benoitstraat,352,,2018,ANTWERPEN,BE,
+ML,De Jaeger,Andr├â┬⌐,Rue des Escargots,4,,7080,FRAMERIES,BE,19470307,,HAVRE,BE,1002,,,160,Fries,D├â┬⌐borah,Rue Emile Vandervelde,104,,7033,MONS,BE,
+ML,Dutoit,Elias,Graaf Boudewijn I-straat,55,,8530,HARELBEKE,BE,20071017,,KORTRIJK,BE,1001,,,,Dutoit,Benny,Graaf Boudewijn I-straat,55,,8530,HARELBEKE,BE,
+ML,Lacroix,Jean-Fran├â┬ºois,Berkendallaan,79,1,1800,VILVOORDE,BE,19831126,,BRUSSEL,BE,1001,,,160,Van Borm,Lydia,Cardijnstraat,35,,1980,ZEMST,BE,
+ML,Dutoit,Elias,Graaf Boudewijn I-straat,55,,8530,HARELBEKE,BE,20071017,,KORTRIJK,BE,1001,,,,Beuselinck,Silvie,Graaf Boudewijn I-straat,55,,8530,HARELBEKE,BE,
+FEML,Mensch,Ria,VIIde-Olympiadelaan,46,,2020,ANTWERPEN,BE,19500904,,WILRIJK,BE,1001,,,,Brys,Steve,Halewijnlaan,55,1002,2050,ANTWERPEN,BE,
+ML,Laridon,Willy,Rollegemstraat,17,,8880,LEDEGEM,BE,19380916,,IZEGEM,BE,1002,,,,Laridon,Stefaan,Tieltstraat,184,,8700,TIELT,BE,
+,Neukelmance,Fernande,Rue de Mortagne,52,,7904,LEUZE-EN-HAINAUT,BE,19370609,,THUMAIDE,BE,1001,,,,Procureur,Fabienne,Rue de Maulde,19,,7534,TOURNAI,BE,
+FEML,Ahmadi,Fatima,Plein,3,2,3700,TONGEREN,BE,,,Mazar E Sharif,AF,1002,,,160,Michiels,Georges,Elisabethwal,23,,3700,TONGEREN-BORGLOON,BE,
+,Kousonsanong,Daophasouk,Haagbeuk,8,,1730,ASSE,BE,19610404,,Savanakhet,LA,1001,,,160,Fayt,Annelies,Bloklaan,44,,1730,ASSE,BE,
+ML,Dobbelaere,Thibault,Golfstraat,17,,9920,LIEVEGEM,BE,19910117,,GENT,BE,1002,,,160,Claerhout,Nicolas,Kerkstraat,3,,9950,LIEVEGEM,BE,
+ML,Stienon Du Pre,Herv├â┬⌐,Rue des Rabots,27,,1460,ITTRE,BE,19320725,,BRUXELLES,BE,1001,,,160,Havet,J├â┬⌐r├â┬┤me,Avenue Reine Astrid,78,,1410,WATERLOO,BE,
+ML,St├â┬⌐b├â┬⌐,Constant,Terlindenvijverstraat,14,,1730,ASSE,BE,19481018,,BRUSSEL,BE,1001,,,160,Fayt,Annelies,Bloklaan,44,,1730,ASSE,BE,
+FEML,Huysman,Marie,Hutsepotstraat,29,,9052,GENT,BE,19480309,,GENT,BE,1001,,,160,Eliano,Isabelle,Torrekensstraat,60,,9820,MERELBEKE-MELLE,BE,
+ML,Renders,Leo,Lierbaan,59,1,2580,PUTTE,BE,19441008,,KASTERLEE,BE,1001,,,160,Vannueten,Nancy,Lierbaan,209,,2580,PUTTE,BE,
+FEML,Swerts,Claudine,Rue Reine Astrid,54,2,1480,TUBIZE,BE,19500502,,LIEGE,BE,1002,,,160,Jantea,Mihaela,Avenue du Japon,35,3,1420,BRAINE-L'ALLEUD,BE,
+ML,Collin,Joseph,Rue du Broctia,50,,5020,NAMUR,BE,19500208,,NAMUR,BE,1001,,,,Collin,Marie,Rue des Crayats,1,,5150,FLOREFFE,BE,
+FEML,Baligant,Christiane,Avenue Trigodet,12,,1401,NIVELLES,BE,,,,,1001,,,,De Boitselier,Christian,Chauss├â┬⌐e de Nivelles,76,,1472,GENAPPE,BE,
+ML,Mahieu,Jan,Hoornstraat,6,21,1500,HALLE,BE,19400710,,DEURNE,BE,1001,,,160,Coene,Geert,Ninoofsesteenweg,177-179,,1700,DILBEEK,BE,
+ML,Dubus,Bernard,Doorniksesteenweg,8,0201,8580,AVELGEM,BE,19510124,,AVELGEM,BE,1001,,,160,De Geeter,Stefaan,Beheerstraat,70,,8500,KORTRIJK,BE,
+FEML,Vanderclause,Andr├â┬⌐e,Rue L├â┬⌐on Defuisseaux,19,0011,7080,FRAMERIES,BE,19500202,,JUMET,BE,1001,,,160,Liegeois,Vincent,Rue Joseph Dufrane,12,,7080,FRAMERIES,BE,
+ML,Bourdouxhe,Emile,Rue Henri Francotte,18,,4607,DALHEM,BE,19541018,,CHERATTE,BE,1001,,,160,Evrard,Jean-Yves,Quai Edouard Van Beneden,4,,4020,LIEGE,BE,
+FEML,Hoefkens,Amelia,Heiken,56,,3202,AARSCHOT,BE,19390217,,HERENTHOUT,BE,1002,,,160,Peeters,Gunther,Herseltsesteenweg,5 A,,3200,AARSCHOT,BE,
+FEML,Verbruggen,Greta,Ren├â┬⌐ Verbeecklaan,7,0204,2540,HOVE,BE,19490613,,MORTSEL,BE,1001,,,160,Van Wesemael,Sofie,Van Putlei,120,,2547,LINT,BE,
+ML,Ghisa,Williams,Place de Seurre,7,,5570,BEAURAING,BE,20020217,,ATH,BE,1001,,,,Ghisa,Florian,All├â┬⌐e du Stade,5C,,5570,BEAURAING,BE,
+ML,Pecheur,Fernand,Place des Chasseurs Ardennais,201,,6740,ETALLE,BE,19440914,,,,1001,,,,Pecheur,Murielle,Rue des Alli├â┬⌐s,15A,,6792,HALANZY,BE,
+FEML,Berkova,Emilia,Beekstraat,46,106,9031,DRONGEN,BE,19960629,,Kosice,SK,1001,,,160,Provenier,Sofie,Dekenijstraat,30,,9930,LIEVEGEM,BE,
+FEML,Lambert,Nelly,Rue de Frameries,37,,7040,QUEVY,BE,19430215,,FRAMERIES,BE,1001,,,160,Beauvois,Xavier,Chemin du Versant,72,,7000,MONS,BE,
+FEML,Collin,Jeanne,Rue du Hanois,1,,6140,FONTAINE-L'EVEQUE,BE,19440819,,JUMET,BE,1002,,,160,Palate,St├â┬⌐phanie,Boulevard Audent,11,3,6000,CHARLEROI,BE,
+FEML,Depoorter,Patricia,Rue de l'H├â┬┤pital,9,,6030,CHARLEROI,BE,19590721,,ACOZ,BE,1002,,,160,Palate,St├â┬⌐phanie,Boulevard Audent,11,3,6000,CHARLEROI,BE,
+FEML,Opdebeeck,Simonne,Weynesbaan,164,,2820,BONHEIDEN,BE,19460715,,MECHELEN,BE,1001,,,160,Van Weerdt,Elke,Frederik de Merodestraat,94-96,,2800,MECHELEN,BE,
+FEML,Lakiere,Rosette,Begonialaan,1,,8890,MOORSLEDE,BE,19431208,,ZONNEBEKE,BE,1001,,,160,Bonte,Vincent,Secretaris Vanmarckelaan,25,,8560,WEVELGEM,BE,
+FEML,Ruysseveldt,Alice,Bollestraat,10,,1741,TERNAT,BE,19350322,,NINOVE,BE,1001,,,160,Vandermotten,Ann,Nattestraat,12 B,,1740,TERNAT,BE,
+ML,Dumon,Alain,Merksplassesteenweg,104,61,2310,RIJKEVORSEL,BE,19860630,,BORGERHOUT,BE,1002,,,160,Laforce,Kelly,Peter Benoitstraat,32,,2018,ANTWERPEN,BE,
+FEML,Spiltoir,Colette,Rue des Coquelets,22,,1400,NIVELLES,BE,19351110,,LA LOUVIERE,BE,1001,,,,Chateau,Etienne,Rue Fran├â┬ºois Lebon,40,,1400,NIVELLES,BE,
+ML,Cordijn,Steven,Kamstraat,54,,1602,SINT-PIETERS-LEEUW,BE,19860901,,AALST,BE,1001,,,,Cordijn,S.,Kamstraat,54,,1602,SINT-PIETERS-LEEUW,BE,
+FEML,De Vlieger,Darline,Nieuwenhovenstraat,2,,8730,BEERNEM,BE,19700323,,BEERNEM,BE,1002,,,,De Vlieger,Koen,Meersestraat,15,,9667,HOREBEKE,BE,
+FEML,Jacquemin,Gis├â┬¿le,Rue du March├â┬⌐,6,,6840,NEUFCHATEAU,BE,19450528,,SAINT-VINCENT,BE,1001,,,,Pecheur,Murielle,Rue des Alli├â┬⌐s,15A,,6792,AUBANGE,BE,
+FEML,Urbain,Josiane,Rue Achille Delattre,146,,7340,COLFONTAINE,BE,19480321,,CHIEVRES,BE,1002,,,160,Lesuisse,Olivier,March├â┬⌐ Croix Place,7,,7000,MONS,BE,
+ML,Dutoit,Elias,Graaf Boudewijn I-straat,55,,8530,HARELBEKE,BE,20071017,,KORTRIJK,BE,1001,,,,Beuselinck,Silvie,Graaf Boudewijn I-straat,55,,8530,HARELBEKE,BE,
+ML,Mouton,Rik,Vinkestraat,44,,8560,WEVELGEM,BE,19540410,,KORTRIJK,BE,1001,,,160,Bonte,Vincent,Secretaris Vanmarckelaan,25,,8560,WEVELGEM,BE,
+FEML,Medart,Lindsay,Rue de la Jonchaie,40,,4032,LIEGE,BE,19921102,,,,1001,,,160,Fontaine,Thibault,Chauss├â┬⌐e d'Argenteau,54,,4601,VISE,BE,
+ML,Neefs,Mathieu,Tulpenlaan,9,1,1880,KAPELLE-OP-DEN-BOS,BE,19780126,,BONHEIDEN,BE,1001,,,160,Verhaeghe,Eline,Hoeveland,32,,1850,GRIMBERGEN,BE,
+FEML,De Guissm├â┬⌐,Sandrine,Chemin de la Basse Couture,13B,,7860,LESSINES,BE,19810110,,LESSINES,BE,1001,,,,De Guissm├â┬⌐,Daniel,Chemin de la Basse Couture,13B,,7860,LESSINES,BE,
+FEML,Depraetere,Veerle,Beselarestraat,1,,8940,WERVIK,BE,19640927,,WEVELGEM,BE,1001,,,160,Deceuninck,Luc,Bruggestraat,55,,8930,MENEN,BE,
+ML,Goessens,Michel,Rue des Usines,43,11,6791,AUBANGE,BE,19471231,,BRUXELLES,BE,1001,,,160,Guillaume,B├â┬⌐reng├â┬¿re,Avenue de Longwy,388,,6700,ARLON,BE,
diff --git a/DataDistribution/src/main/resources/CofaceLEChild.csv b/DataDistribution/src/main/resources/CofaceLEChild.csv
index bcbfaa3..83473a1 100644
--- a/DataDistribution/src/main/resources/CofaceLEChild.csv
+++ b/DataDistribution/src/main/resources/CofaceLEChild.csv
@@ -1,5 +1,2 @@
 "LE_ExternalIdentifier","IndustryClass_Code","IndustryClass_Rank","IndustryClass_EffDate","IndustryClass_EndDate"
-"1017306504","84114","1","01-03-2002",
-"1017306504","15200","1","23-05-1947",
-"1017306504","47721","1","01-01-2008",
-3
\ No newline at end of file
+"0628484972","71209","1","10/04/2015","10/04/2026"
\ No newline at end of file
diff --git a/DataDistribution/src/main/resources/cofaceLEParent.csv b/DataDistribution/src/main/resources/cofaceLEParent.csv
index 1ed5ca1..d6eb7b4 100644
--- a/DataDistribution/src/main/resources/cofaceLEParent.csv
+++ b/DataDistribution/src/main/resources/cofaceLEParent.csv
@@ -1,5 +1,2 @@
-"LE_ExternalIdentifier","ORG_Status","ORG_Name","ORG_OtherName","ORG_OtherNameType","Adr_UnstructuredAddress","Adr_PostalAddress_StreetName","Adr_PostalAddress_HouseNum","Adr_PostalAddress_HouseNumAdd","Adr_PostalAddress_PostalCode","Adr_PostalAddress_CityName","Adr_PostalAddress_CountryCode","ORG_LegalForm","ORG_BusinessClosedownDate","LE_LegalStatus","ASMT_IdVerif_ApprovalDate","LE_PreferredLanguage","Adr_DigitalAddr_FullTel","NSSO_ExternalIdentifier","ASMT_VAT_Status","NSSO_ExternalIdentifierStatus"
-"1017306504","AC","VOITHUUR","","","Moenkouterstraat 5","Moenkouterstraat","5","","8552","MOEN","","014","","000","","3",""+3265654566"","180774437","0","0"
-"1001841041","AC","JONCKHEERE.WOOD","","","Z. 5 Mollem 5","Z. 5 Mollem","5","","1730","MOLLEM","","014","","000","","3",""+3224540330"","","0","0"
-"1001000001","AC","ABC Industries NV","ABC IND","abbrev","Koningsstraat 100","Koningsstraat","100","","1000","BRUSSEL","BE","014","","000","","1","+3222223344","100000001","0","0"
-3
\ No newline at end of file
+"LE_ExternalIdentifier","ORG_Status","ORG_Name","ORG_OtherName","ORG_OtherNameType","Adr_UnstructuredAddress","Adr_PostalAddress_StreetName","Adr_PostalAddress_HouseNum","Adr_PostalAddress_HouseAdd","Adr_PostalAddress_PostalCode","Adr_PostalAddress_CityName","Adr_PostalAddress_CntryCd","ORG_LegalForm","ORG_BusinessClosedownDate","LE_LegalStatus","ORG_DateOfFoundation","LE_PreferredLanguage","Adr_DigitalAddr_FullTel","NSSO_ExternalIdentifier","ASMT_VAT_Status","NSSO_ExternalIdentifierStatus"
+"0628484972","AC","Quality Inspection Services","QIS","abbrev","Michiganstraat 16 B.3","Michiganstraat","16","3","2030","ANTWERPEN","BE","610","10/04/2026","000","10/04/2015","3","03/820.52.50","191104840","1","0"
\ No newline at end of file
diff --git a/DataDistribution/src/main/resources/cofaceOpsData.csv b/DataDistribution/src/main/resources/cofaceOpsData.csv
index 119b49c..823e234 100644
--- a/DataDistribution/src/main/resources/cofaceOpsData.csv
+++ b/DataDistribution/src/main/resources/cofaceOpsData.csv
@@ -1,4 +1,5 @@
-"LE_EXTERNALIDENTIFIER_VAL","OPS_EXTERNALIDENTIFIER_VAL","OPS_ORGANISATIONUNITNAME","ADR_POSTALADDRESS_STREETNM","ADR_POSTALADDRESS_HOUSENUM","ADR_POSTALADDRESS_HOUSEADD","ADR_POSTALADDRESS_POSTALCD","ADR_POSTALADDRESS_CITYNAME","ADR_POSTALADDRESS_CNTRYCD","OPS_EXTID_DATEOFISSUE","OPS_EXTID_EXPIRYDATE","ADR_DIGITALADDR_FULLTEL","ADR_DIGITALADDR_FULLTELFGN","ADR_DIGITALADDR_EMAIL","ADR_DIGITALADDR_FULLMOB
-"1001841041","2351593071","Rudolf Events","Lange Dijkstraat","81","28","9200","SINT-GILLIS-DENDERMONDE","0","31-10-2023","12-12-2024","123456789","","test","12345678"
-"1017306504","2367934702","Sicli Fire Protection South","Industriepark","24","2","2220","HEIST-OP-DEN-BERG","0","12-12-2024","10-12-2025","1234567890","","test","123456789"
-2
\ No newline at end of file
+LE_EXTERNALIDENTIFIER_VAL,OPS_EXTERNALIDENTIFIER_VAL,OPS_ORGANISATIONUNITNAME,ADR_POSTALADDRESS_STREETNM,ADR_POSTALADDRESS_HOUSENUM,ADR_POSTALADDRESS_HOUSEADD,ADR_POSTALADDRESS_POSTALCD,ADR_POSTALADDRESS_CITYNAME,ADR_POSTALADDRESS_CNTRYCD,OPS_EXTID_DATEOFISSUE,OPS_EXTID_EXPIRYDATE,ADR_DIGITALADDR_FULLTEL,ADR_DIGITALADDR_FULLTELFGN,ADR_DIGITALADDR_EMAIL,ADR_DIGITALADDR_FULLMOB
+1001841041,2351593071,Rudolf Events,Lange Dijkstraat,81,28,9200,SINT-GILLIS-DENDERMONDE,0,31-10-2023,12-12-2024,123456789,,test,12345678
+1017306504,2367934702,Sicli Fire Protection South,Industriepark,24,2,2220,HEIST-OP-DEN-BERG,0,12-12-2024,10-12-2025,1234567890,,test,123456789
+1001000003,2355339746,New Organisation,New Street,10,30,1000,BRUSSELS,0,01-04-2024,,12345678,,test,1234567890
+3
\ No newline at end of file
diff --git a/DataDistribution/src/main/resources/ddsql.sql b/DataDistribution/src/main/resources/ddsql.sql
index c686f72..cc1f1e5 100644
--- a/DataDistribution/src/main/resources/ddsql.sql
+++ b/DataDistribution/src/main/resources/ddsql.sql
@@ -843,32 +843,4 @@ ALTER TABLE DD_NOTIFICATION_STATUS ADD (
 ALTER TABLE DD_API_STATUS_TBL DROP(
     TRANSACTION_ID,
     PROCESSING_TYPE
-    );
-
-/* ====== DD_LEGALENTITY_DELETE_TBL TABLE ====== */
-/* Table: DD_LEGALENTITY_DELETE_TBL
-   Purpose: Stores Legal Entity Deletion records from COFACE_LE_DEL.csv
-   Records with LE_DELETIONFLAG='Y' trigger API call to set lifecycle status to DSLV_ORG
-*/
-
-CREATE TABLE DD_LEGALENTITY_DELETE_TBL (
-    ID                      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
-    LE_EXTERNAL_IDENTIFIER  VARCHAR2(50) NOT NULL,
-    LE_DELETION_FLAG        VARCHAR2(1) NOT NULL,
-    DD_UUID                 VARCHAR2(50),
-    FILE_ID                 VARCHAR2(50),
-    CREATED_BY              VARCHAR2(50) DEFAULT 'DD_USER',
-    CREATED_TS              TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
-    UPDATED_BY              VARCHAR2(50) DEFAULT 'DD_USER',
-    UPDATED_TS              TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
-    CONSTRAINT FK_LE_DEL_FILE FOREIGN KEY (FILE_ID) REFERENCES DD_FILE_INGESTION_TBL(FILE_ID)
-);
-
--- Indexes for performance
-CREATE INDEX IDX_LE_DEL_EXT_ID ON DD_LEGALENTITY_DELETE_TBL(LE_EXTERNAL_IDENTIFIER);
-CREATE INDEX IDX_LE_DEL_FILE_ID ON DD_LEGALENTITY_DELETE_TBL(FILE_ID);
-CREATE INDEX IDX_LE_DEL_DD_UUID ON DD_LEGALENTITY_DELETE_TBL(DD_UUID);
-
-ALTER TABLE DD_LEGALENTITY_DELETE_TBL DROP(
-    LE_EXTERNAL_IDENTIFIER
-    );
+    )
\ No newline at end of file
diff --git a/LocalConf/src/main/resources/application.yml b/LocalConf/src/main/resources/application.yml
index ee23260..ba3d906 100644
--- a/LocalConf/src/main/resources/application.yml
+++ b/LocalConf/src/main/resources/application.yml
@@ -6,7 +6,6 @@ app:
   le-parent-file: classpath:cofaceLEParent.csv
   le-child-file: classpath:CofaceLEChild.csv
   incapables-file: classpath:CofaceIncapables.csv
-  le-delete-file: classpath:COFACE_LE_DEL.csv
   apiFlow: true
   initialLoad: false
 ecs:
@@ -43,10 +42,9 @@ spring:
     driver-class-name: oracle.jdbc.OracleDriver
   batch:
     schedular:
-      legalEntityCron: "0 */2 * * * *"
-      organisationUnitCron: "0 */2 * * * *"
-      incapablesCron: "0 */2 * * * *"
-      leDeletionCron: "0 */2 * * * *"
+      legalEntityCron: "0 */1 * * * *"
+      organisationUnitCron: "0 */1 * * * *"
+      incapablesCron: "0 */1 * * * *"
       notificationStatusCron: "0 0 0 * * *"
       notificationFailStatusCron: "0 0 0 * * *"
       notificationTimeOutStatusCron: "0 0 0 * * *"
@@ -117,14 +115,6 @@ kafka:
       topic-name: "P00007.ingbeextnotifyorganisationname5topic"
     assessment:
       topic-name: "P00007.ingbeextnotifyassessment5topic"
-    incapable-individual:
-      topic-name: "P00007.ingbeextnotifyindividual5topic"
-    incapable-individual-name:
-      topic-name: "P00007.ingbeextnotifyindividualname5topic"
-    incapable-occupation:
-      topic-name: "P00007.ingbeextnotifyoccupation5topic"
-    incapable-marking:
-      topic-name: "P00007.ingbeextnotifyinvolvedpartymarking5topic"
 
 
 merak:
@@ -260,8 +250,8 @@ http-client:
 #=======================================================================================================================
 data-retention:
   enabled: true
-  retention-days: 1
-  cleanup-cron: "0 */50 * * * *"
+  retention-days: 30
+  cleanup-cron: "0 */30 * * * *"
   batch-size: 100
   delete-s3-files: true
 
@@ -323,7 +313,4 @@ data-retention:
       enabled: true
       description: "Final notification status records"
 
-    - name: DD_LEGALENTITY_DELETE_TBL
-      file-id-field: FILE_ID
-      enabled: true
-      description: "LE Deletion Records"
\ No newline at end of file
+
diff --git a/ichpConf/src/main/resources/ichp/acc/properties/dcr/application-default.yml b/ichpConf/src/main/resources/ichp/acc/properties/dcr/application-default.yml
index f1c9bdf..d274951 100644
--- a/ichpConf/src/main/resources/ichp/acc/properties/dcr/application-default.yml
+++ b/ichpConf/src/main/resources/ichp/acc/properties/dcr/application-default.yml
@@ -5,7 +5,6 @@ app:
   input-file: classpath:cofaceOpsData.csv
   le-parent-file: classpath:cofaceLEParent.csv
   le-child-file: classpath:CofaceLEChild.csv
-  incapables-file: classpath:CofaceIncapables.csv
   apiFlow: true
   initialLoad: false
 ecs:
@@ -19,7 +18,6 @@ ecs:
   cofaceOUFileName: cofaceOpsData.csv
   cofaceLEParentFileName: cofaceLEParent.csv
   cofaceLEChildFileName: CofaceLEChild.csv
-  cofaceIncapablesFileName: CofaceIncapables.csv
   readFileFrom: ECS
   s3:
     enabled: true
@@ -249,6 +247,7 @@ spring:
       notificationStatusCron: "0 0 0 * * *"
       notificationFailStatusCron: "0 0 0 * * *"
       notificationTimeOutStatusCron: "0 0 0 * * *"
+
     job:
       name: organisationUnitEnrichmentJob
       enabled: false
@@ -277,14 +276,6 @@ kafka:
       topic-name: "P00007.ingbeextnotifyorganisationname5topic"
     assessment:
       topic-name: "P00007.ingbeextnotifyassessment5topic"
-    incapable-individual:
-      topic-name: "P00007.ingbeextnotifyindividual5topic"
-    incapable-individual-name:
-      topic-name: "P00007.ingbeextnotifyindividualname5topic"
-    incapable-occupation:
-      topic-name: "P00007.ingbeextnotifyoccupation5topic"
-    incapable-marking:
-      topic-name: "P00007.ingbeextnotifyinvolvedpartymarking5topic"
 
 
 notifications:
diff --git a/ichpConf/src/main/resources/ichp/acc/properties/wpr/application-default.yml b/ichpConf/src/main/resources/ichp/acc/properties/wpr/application-default.yml
index ee8a1e5..a929171 100644
--- a/ichpConf/src/main/resources/ichp/acc/properties/wpr/application-default.yml
+++ b/ichpConf/src/main/resources/ichp/acc/properties/wpr/application-default.yml
@@ -4,10 +4,7 @@
 app:
   input-file: classpath:cofaceOpsData.csv
   le-parent-file: classpath:cofaceLEParent.csv
-  le-child-file: classpath:CofaceLEChild.csv'
-  incapables-file: classpath:CofaceIncapables.csv
-  le-delete-file: classpath:COFACE_LE_DEL.csv
-
+  le-child-file: classpath:CofaceLEChild.csv
   apiFlow: true
   initialLoad: false
 ecs:
@@ -21,9 +18,6 @@ ecs:
   cofaceOUFileName: cofaceOpsData.csv
   cofaceLEParentFileName: cofaceLEParent.csv
   cofaceLEChildFileName: CofaceLEChild.csv
-  cofaceIncapablesFileName: CofaceIncapables.csv
-  cofaceLEDeleteFileName: COFACE_LE_DEL.csv
-
   readFileFrom: ECS
   s3:
     enabled: true
@@ -250,8 +244,6 @@ spring:
     schedular:
       legalEntityCron: "0 */1 * * * *"
       organisationUnitCron: "0 */5 * * * *"
-      incapablesCron: "0 */5 * * * *"
-      leDeletionCron: "0 */1 * * * *"
       notificationStatusCron: "0 0 0 * * *"
       notificationFailStatusCron: "0 0 0 * * *"
       notificationTimeOutStatusCron: "0 0 0 * * *"
diff --git a/ichpConf/src/main/resources/ichp/tst/properties/wpr/application-default.yml b/ichpConf/src/main/resources/ichp/tst/properties/wpr/application-default.yml
index 98c0628..7037ffa 100644
--- a/ichpConf/src/main/resources/ichp/tst/properties/wpr/application-default.yml
+++ b/ichpConf/src/main/resources/ichp/tst/properties/wpr/application-default.yml
@@ -2,12 +2,10 @@
 # Spring Configuration
 #=======================================================================================================================
 app:
-  input-file: classpath:cofaceOpsData.csv
+  input-file: classpath:cofaceOpsData_INS_OPSAPI_TC001.csv
   le-parent-file: classpath:cofaceLEParent.csv
   le-child-file: classpath:CofaceLEChild.csv
   incapables-file: classpath:CofaceIncapables.csv
-  le-delete-file: classpath:COFACE_LE_DEL.csv
-
   apiFlow: true
   initialLoad: false
 ecs:
@@ -18,7 +16,7 @@ ecs:
   uploadBucketName: OnePam
   outputFilePath: DIFiles
   output-file: OilFiles
-  cofaceOUFileName: cofaceOpsDataTest.csv
+  cofaceOUFileName: cofaceOpsData_INS_OPSAPI_TC001.csv
   cofaceLEParentFileName: cofaceLEParent.csv
   cofaceLEChildFileName: CofaceLEChild.csv
   cofaceIncapablesFileName: CofaceIncapables.csv
@@ -248,11 +246,10 @@ spring:
     schedular:
       legalEntityCron: "0 */1 * * * *"
       organisationUnitCron: "0 */1 * * * *"
+      incapablesCron: "0 0 * * * *"
       notificationStatusCron: "0 0 0 * * *"
       notificationFailStatusCron: "0 0 0 * * *"
       notificationTimeOutStatusCron: "0 0 0 * * *"
-      incapablesCron: "0 */1 * * * *"
-      leDeletionCron: "0 */1 * * * *"
 
     job:
       name: organisationUnitEnrichmentJob
@@ -282,14 +279,6 @@ kafka:
       topic-name: "P00007.ingbeextnotifyorganisationname5topic"
     assessment:
       topic-name: "P00007.ingbeextnotifyassessment5topic"
-    incapable-individual:
-      topic-name: "P00007.ingbeextnotifyindividual5topic"
-    incapable-individual-name:
-      topic-name: "P00007.ingbeextnotifyindividualname5topic"
-    incapable-occupation:
-      topic-name: "P00007.ingbeextnotifyoccupation5topic"
-    incapable-marking:
-      topic-name: "P00007.ingbeextnotifyinvolvedpartymarking5topic"
 
 
 notifications:
@@ -314,8 +303,8 @@ sdm.markings.reference.data.table.name: IP_MRKNG_TYP
 #=======================================================================================================================
 data-retention:
   enabled: true
-  retention-days: 30
-  cleanup-cron: "0 */50 * * * *"
+  retention-days: 0
+  cleanup-cron: "* */50 * * * *"
   batch-size: 100
   delete-s3-files: true
 
@@ -376,9 +365,5 @@ data-retention:
       file-id-field: FILE_ID
       enabled: true
       description: "Final notification status records"
-      
-    - name: DD_LEGALENTITY_DELETE_TBL
-      file-id-field: FILE_ID
-      enabled: true
-      description: "LE Deletion Records"
+
 
origin/faeture/test/11781059-systemtest-ops-le:
has no parsing error
but develop has
tell why how where wht to do
