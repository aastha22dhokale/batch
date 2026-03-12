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
package com.ing.datadist.batch.schedular;

import com.ing.datadist.dao.ErrorLogDAO;
import com.ing.datadist.enums.ErrorType;
import com.ing.datadist.enums.RecordType;
import com.ing.datadist.service.FileIngestionService;
import com.ing.datadist.util.CsvTotalRecordsParser;
import com.ing.datadist.util.FileUtility;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
public class BatchSchedular {

    private final JobLauncher jobLauncher;
    private final Job cofaceOUjob;
    private final Job cofaceLEJob;
    private final Job incapablesJob;
    private final Job leDeletionJob;
    private final JobExplorer jobExplorer;
    private final ErrorLogDAO errorLogDAO;
    private final FileIngestionService fileIngestionService;
    private final AtomicBoolean organisationUnitBatchRunning = new AtomicBoolean(false);
    private final AtomicBoolean legalEntityBatchRunning = new AtomicBoolean(false);
    private final AtomicBoolean incapablesBatchRunning = new AtomicBoolean(false);
    private final AtomicBoolean leDeletionBatchRunning = new AtomicBoolean(false);
    Boolean runFlagOU = true;
    Boolean runFlagLE = true;
    Boolean runFlagIncapables = true;
    Boolean runFlagLEDeletion = true;


    @Value("${spring.batch.schedular.organisationUnitCron}")
    private String organisationUnitCron;

    @Value("${spring.batch.schedular.legalEntityCron}")
    private String legalEntityCron;

    @Value("${spring.batch.schedular.incapablesCron}")
    private String incapablesCron;

    @Value("${spring.batch.schedular.leDeletionCron:0 */5 * * * *}")
    private String leDeletionCron;

    @Value("${ecs.cofaceIncapablesFileName}")
    private String cofaceIncapablesFileName;

    @Value("${ecs.cofaceOUFileName}")
    private String cofaceOUFileName;

    @Value("${ecs.cofaceLEParentFileName}")
    private String cofaceLEParentFileName;

    @Value("${ecs.cofaceLEChildFileName}")
    private String cofaceLEChildFileName;

    @Value("${ecs.cofaceLEDeleteFileName:COFACE_LE_DEL.csv}")
    private String cofaceLEDeleteFileName;

    @Value("${ecs.readFileFrom}")
    private String readFileFrom;


    public BatchSchedular(JobLauncher jobLauncher,
                          @Qualifier("organisationUnitEnrichmentJob") Job job,
                          @Qualifier("LegalEntityEnrichmentJob") Job cofaceLEJob,
                          @Qualifier("IncapablesEnrichmentJob") Job incapablesJob,
                          @Qualifier("LegalEntityDeletionJob") Job leDeletionJob,
                          JobExplorer jobExplorer, ErrorLogDAO errorLogDAO, FileIngestionService fileIngestionService) {    this.jobLauncher = jobLauncher;
        this.cofaceOUjob = job;
        this.cofaceLEJob = cofaceLEJob;
        this.incapablesJob = incapablesJob;
        this.leDeletionJob = leDeletionJob;
        this.jobExplorer = jobExplorer;
        this.errorLogDAO = errorLogDAO;
        this.fileIngestionService = fileIngestionService;
    }

    @Scheduled(cron = "${spring.batch.schedular.organisationUnitCron}")
    public void run() {
        log.info("OrganisationUnit Batch Schedular started:{}", LocalDateTime.now());
        JobExecution execution = null;
        String fileId = null;
        String fileName = null;

        if (!organisationUnitBatchRunning.compareAndSet(false, true)) {
            log.info("OrganisationUnit Batch still running, skipping this run :{}", LocalDateTime.now());
            return;
        }
        if (runFlagOU) {
            try {
                log.info("OrganisationUnit Batch Schedular cron:{} started:{}", organisationUnitCron, LocalDateTime.now());

                // Fetch file name from configuration
                fileName = cofaceOUFileName;
                String filePath = buildFilePath(fileName);

                // Calculate total records from CSV file BEFORE registration

                Long ouTotalRecords = CsvTotalRecordsParser.getTotalRecordsFromCsv(filePath);
                Long ouFileSize = CsvTotalRecordsParser.getFileSizeInBytes(filePath);

                log.info("OrganisationUnit File: {} - Total records from file: {}, File size: {}",
                        fileName, ouTotalRecords, ouFileSize);

                fileId = fileIngestionService.registerFileIngestion(
                        fileName,
                        "COFACE_OPS",
                        "OPS",
                        "COFACE_SYSTEM",
                        "DAILY",
                        ouTotalRecords
                );

                JobParameters jobParameters = new JobParametersBuilder(jobExplorer)
                        .addLong("run.id", System.currentTimeMillis())
                        .addString("opsFileId", fileId)
                        .addString("fileName", fileName)
                        .toJobParameters();

                // Mark file processing start
                fileIngestionService.markFileProcessingStart(fileId, String.valueOf(System.currentTimeMillis()));

                execution = jobLauncher.run(cofaceOUjob, jobParameters);

                // Update file processing completion based on execution status
                String fileChecksum = FileUtility.calculateMD5Checksum(filePath);
                String formattedSize = FileUtility.formatFileSize(ouFileSize);

                if (execution.getStatus().toString().equals("COMPLETED")) {
                    // Get record counts from job execution context
                    Long totalRecords = getTotalRecordsFromExecution(execution);
                    Long processedRecords = getProcessedRecordsFromExecution(execution);
                    Long failedRecords = getFailedRecordsFromExecution(execution);

                    // If execution context doesn't have counts, use parsed value for total
                    if (totalRecords == 0L && ouTotalRecords > 0L) {
                        totalRecords = ouTotalRecords;
                    }

                    // For successful completion, if processedRecords is 0, assume all records were processed
                    if (processedRecords == 0L && totalRecords > 0L) {
                        processedRecords = totalRecords;
                    }

                    // If no failures logged, set to 0
                    if (failedRecords == null) {
                        failedRecords = 0L;
                    }

                    log.info("OrganisationUnit Batch completed - fileSize: {}, fileChecksum: {}, totalRecords: {}, processedRecords: {}, failedRecords: {}",
                            formattedSize, fileChecksum, totalRecords, processedRecords, failedRecords);

                    fileIngestionService.updateFileProcessingEnd(fileId, ouFileSize, fileChecksum,
                            totalRecords, processedRecords, failedRecords);
                } else {
                    String errorMsg = "Batch execution failed with status: " + execution.getStatus();
                    log.error("OrganisationUnit Batch failed: {}", errorMsg);
                    fileIngestionService.markFileProcessingFailed(fileId, errorMsg);
                }
            } catch (Exception e) {
                log.error("Exception in OrganisationUnit Batch Schedular", e);

                // Log to DD_ERROR_LOG table
                try {
                    StringWriter sw = new StringWriter();
                    e.printStackTrace(new PrintWriter(sw));
                    errorLogDAO.logError(
                            RecordType.ORGANISATION_UNIT,
                            "BATCH_JOB",
                            null,
                            ErrorType.PARSING_ERROR,
                            "Batch Scheduler Exception: " + e.getMessage(),
                            null,
                            fileName,
                            sw.toString(),
                            fileId  // File ID
                    );
                } catch (Exception logEx) {
                    log.error("Failed to log batch scheduler error", logEx);
                }

                // Mark file as failed
                if (fileId != null) {
                    try {
                        fileIngestionService.markFileProcessingFailed(fileId, "Exception in batch scheduler: " + e.getMessage());
                    } catch (Exception fileEx) {
                        log.error("Failed to mark file as failed", fileEx);
                    }
                }
            } finally {
                if (execution != null) {
                    log.info("OrganisationUnit Batch Schedular finished name:{} status:{}", cofaceOUjob.getName(), execution.getStatus());
                } else {
                    log.info("OrganisationUnit Batch Schedular failed name:{}", cofaceOUjob.getName());
                }
                organisationUnitBatchRunning.set(false);
            }
        }else{
            log.info("OrganisationUnit Batch Schedular flag stopped");
        }
        runFlagOU =  false;
    }

    @Scheduled(cron = "${spring.batch.schedular.legalEntityCron}")
    public void runLE() {
        log.info("LegalEntity Batch Schedular started:{}", LocalDateTime.now());
        JobExecution execution = null;
        String parentFileId = null;
        String childFileId = null;
        String parentFileName = null;
        String childFileName = null;

        if (!legalEntityBatchRunning.compareAndSet(false, true)) {
            log.info("LegalEntity Batch still running, skipping this run :{}", LocalDateTime.now());
            return;
        }
        if(runFlagLE) {
            try {
                log.info("LegalEntity Batch Schedular cron:{} started:{}", legalEntityCron, LocalDateTime.now());

                // Fetch PARENT file name from configuration
                parentFileName = cofaceLEParentFileName;
                String parentFilePath = buildFilePath(parentFileName);

                Long parentTotalRecords = CsvTotalRecordsParser.getTotalRecordsFromCsv(parentFilePath);
                Long parentFileSize = CsvTotalRecordsParser.getFileSizeInBytes(parentFilePath);

                parentFileId = fileIngestionService.registerFileIngestion(
                        parentFileName,
                        "COFACE_LE_PARENT",
                        "LE",
                        "COFACE_SYSTEM",
                        "DAILY",
                        parentTotalRecords
                );

                // Fetch CHILD file name from configuration
                childFileName = cofaceLEChildFileName;
                String childFilePath = buildFilePath(childFileName);

                Long childTotalRecords = CsvTotalRecordsParser.getTotalRecordsFromCsv(childFilePath);
                Long childFileSize = CsvTotalRecordsParser.getFileSizeInBytes(childFilePath);

                childFileId = fileIngestionService.registerFileIngestion(
                        childFileName,
                        "COFACE_LE_CHILD",
                        "LE",
                        "COFACE_SYSTEM",
                        "DAILY",
                        childTotalRecords
                );

                JobParameters jobParameters = new JobParametersBuilder(jobExplorer)
                        .addLong("run.id", System.currentTimeMillis())
                        .addString("leParentFileId", parentFileId)
                        .addString("leChildFileId",  childFileId)
                        .addString("leParentFileName", parentFileName)
                        .addString("leChildFileName", childFileName)
                        .toJobParameters();

                // Mark file processing start
                fileIngestionService.markFileProcessingStart(parentFileId, String.valueOf(System.currentTimeMillis()));
                fileIngestionService.markFileProcessingStart(childFileId, String.valueOf(System.currentTimeMillis()));

                execution = jobLauncher.run(cofaceLEJob, jobParameters);

                // Update file processing completion based on execution status
                String parentFileChecksum = FileUtility.calculateMD5Checksum(parentFilePath);
                String parentFormattedSize = FileUtility.formatFileSize(parentFileSize);

                String childFileChecksum = FileUtility.calculateMD5Checksum(childFilePath);
                String childFormattedSize = FileUtility.formatFileSize(childFileSize);

                if (execution.getStatus().toString().equals("COMPLETED")) {
                    // Get record counts from job execution context
                    Long totalRecords = getTotalRecordsFromExecution(execution);
                    Long processedRecords = getProcessedRecordsFromExecution(execution);
                    Long failedRecords = getFailedRecordsFromExecution(execution);

                    // If execution context doesn't have counts, use parsed values
                    if (totalRecords == 0L && parentTotalRecords > 0L) {
                        totalRecords = parentTotalRecords;
                    }

                    // For successful completion, if processedRecords is 0, assume all records were processed
                    if (processedRecords == 0L && totalRecords > 0L) {
                        processedRecords = totalRecords;
                    }

                    // If no failures logged, set to 0
                    if (failedRecords == null) {
                        failedRecords = 0L;
                    }

                    log.info("LegalEntity Batch completed - PARENT fileSize: {}, fileChecksum: {}, CHILD fileSize: {}, fileChecksum: {}, totalRecords: {}, processedRecords: {}, failedRecords: {}",
                            parentFormattedSize, parentFileChecksum, childFormattedSize, childFileChecksum, totalRecords, processedRecords, failedRecords);

                    fileIngestionService.updateFileProcessingEnd(parentFileId, parentFileSize, parentFileChecksum,
                            totalRecords, processedRecords, failedRecords);
                    fileIngestionService.updateFileProcessingEnd(childFileId, childFileSize, childFileChecksum,
                            totalRecords, processedRecords, failedRecords);
                } else {
                    String errorMsg = "Batch execution failed with status: " + execution.getStatus();
                    log.error("LegalEntity Batch failed: {}", errorMsg);
                    fileIngestionService.markFileProcessingFailed(parentFileId, errorMsg);
                    fileIngestionService.markFileProcessingFailed(childFileId, errorMsg);
                }
            } catch (Exception e) {
                log.error("Exception in LegalEntity Batch Schedular", e);

                // Log to DD_ERROR_LOG table
                try {
                    StringWriter sw = new StringWriter();
                    e.printStackTrace(new PrintWriter(sw));

                    errorLogDAO.logError(
                            RecordType.ORGANISATION_PARENT,
                            "BATCH_JOB",
                            null,
                            ErrorType.DATABASE_ERROR,
                            "Batch Scheduler Exception: " + e.getMessage(),
                            null,
                            parentFileName,
                            sw.toString(),
                            parentFileId
                    );
                } catch (Exception logEx) {
                    log.error("Failed to log batch scheduler error", logEx);
                }

                // Mark files as failed
                if (parentFileId != null) {
                    try {
                        fileIngestionService.markFileProcessingFailed(parentFileId, "Exception in batch scheduler: " + e.getMessage());
                    } catch (Exception fileEx) {
                        log.error("Failed to mark parent file as failed", fileEx);
                    }
                }
                if (childFileId != null) {
                    try {
                        fileIngestionService.markFileProcessingFailed(childFileId, "Exception in batch scheduler: " + e.getMessage());
                    } catch (Exception fileEx) {
                        log.error("Failed to mark child file as failed", fileEx);
                    }
                }
            } finally {
                if (execution != null) {
                    log.info("LegalEntity Batch Schedular finished name:{} status:{}", cofaceLEJob.getName(), execution.getStatus());
                } else {
                    log.info("LegalEntity Batch Schedular failed name:{}", cofaceLEJob.getName());
                }
                legalEntityBatchRunning.set(false);
            }
        }else{
            log.info("LegalEntity Batch Schedular flag stopped");
        }
        runFlagLE =  false;
    }

    /**
     * Extract total records processed from job execution context
     */
    private Long getTotalRecordsFromExecution(JobExecution execution) {
        try {
            Object totalRecords = execution.getExecutionContext().get("totalRecords");
            return totalRecords != null ? Long.valueOf(totalRecords.toString()) : 0L;
        } catch (Exception e) {
            log.warn("Unable to retrieve totalRecords from job execution context", e);
            return 0L;
        }
    }

    /**
     * Extract processed records from job execution context
     */
    private Long getProcessedRecordsFromExecution(JobExecution execution) {
        try {
            Object processedRecords = execution.getExecutionContext().get("processedRecords");
            return processedRecords != null ? Long.valueOf(processedRecords.toString()) : 0L;
        } catch (Exception e) {
            log.warn("Unable to retrieve processedRecords from job execution context", e);
            return 0L;
        }
    }

    /**
     * Extract failed records from job execution context
     */
    private Long getFailedRecordsFromExecution(JobExecution execution) {
        try {
            Object failedRecords = execution.getExecutionContext().get("failedRecords");
            return failedRecords != null ? Long.valueOf(failedRecords.toString()) : 0L;
        } catch (Exception e) {
            log.warn("Unable to retrieve failedRecords from job execution context", e);
            return 0L;
        }
    }

    /**
     * Build file path based on readFileFrom configuration
     */
    private String buildFilePath(String fileName) {
        if ("RESOURCES".equals(readFileFrom)) {
            return "src/main/resources/" + fileName;
        } else if ("ECS".equals(readFileFrom)) {
            // For ECS, return just the file name - it will be fetched from S3
            return fileName;
        } else {
            // Default to resources
            return "src/main/resources/" + fileName;
        }
    }

    @Scheduled(cron = "${spring.batch.schedular.incapablesCron}")
    public void runIncapables() {
        log.info("Incapables Batch Schedular started:{}", LocalDateTime.now());
        JobExecution execution = null;
        String fileId = null;
        String fileName = null;

        if (!incapablesBatchRunning.compareAndSet(false, true)) {
            log.info("Incapables Batch still running, skipping this run :{}", LocalDateTime.now());
            return;
        }

        if (runFlagIncapables) {
            try {
                log.info("Incapables Batch Schedular cron:{} started:{}", incapablesCron, LocalDateTime.now());

                fileName = cofaceIncapablesFileName;
                String filePath = buildFilePath(fileName);

                Long incapablesTotalRecords = CsvTotalRecordsParser.getTotalRecordsFromCsv(filePath);
                Long incapablesFileSize = CsvTotalRecordsParser.getFileSizeInBytes(filePath);

                log.info("Incapables File: {} - Total records from file: {}, File size: {}",
                        fileName, incapablesTotalRecords, incapablesFileSize);
                fileId = fileIngestionService.registerFileIngestion(
                        fileName,
                        "COFACE_INCAPABLES",
                        "INCAPABLES",
                        "COFACE_SYSTEM",
                        "DAILY",
                        incapablesTotalRecords
                );

                JobParameters jobParameters = new JobParametersBuilder(jobExplorer)
                        .addLong("run.id", System.currentTimeMillis())
                        .addString("incapablesFileId", fileId)
                        .addString("fileName", fileName)
                        .toJobParameters();

                fileIngestionService.markFileProcessingStart(fileId, String.valueOf(System.currentTimeMillis()));

                execution = jobLauncher.run(incapablesJob, jobParameters);


                String fileChecksum = FileUtility.calculateMD5Checksum(filePath);
                String formattedSize = FileUtility.formatFileSize(incapablesFileSize);

                if (execution.getStatus().isUnsuccessful()) {
                    String errorMsg = "Batch execution failed with status: " + execution.getStatus();
                    log.error("Incapables Batch failed: {}", errorMsg);
                    fileIngestionService.markFileProcessingFailed(fileId, errorMsg);
                } else {

                    // *** NEW: read counters from execution context ***
                    Long totalRecords = getTotalRecordsFromExecution(execution);
                    Long processedRecords = getProcessedRecordsFromExecution(execution);
                    Long failedRecords = getFailedRecordsFromExecution(execution);

                    // fallback if job context did not set total
                    if (totalRecords == 0L && incapablesTotalRecords > 0L) {
                        totalRecords = incapablesTotalRecords;
                    }

                    if (processedRecords == 0L && totalRecords > 0L) {
                        processedRecords = totalRecords;
                    }

                    if (failedRecords == null) {
                        failedRecords = 0L;
                    }

                    log.info("Incapables Batch completed - fileSize: {}, checksum: {}, totalRecords: {}, processedRecords: {}, failedRecords: {}",
                            formattedSize, fileChecksum, totalRecords, processedRecords, failedRecords);

                    fileIngestionService.updateFileProcessingEnd(
                            fileId,
                            incapablesFileSize,
                            fileChecksum,
                            totalRecords,
                            processedRecords,
                            failedRecords
                    );
                }

            } catch (Exception e) {
                log.error("Exception in Incapables Batch Schedular", e);
                if (fileId != null) {
                    fileIngestionService.markFileProcessingFailed(fileId, "Exception in batch scheduler: " + e.getMessage());
                }
            } finally {
                log.info("Incapables Batch Schedular finished name:{} status:{}",
                        incapablesJob.getName(),
                        execution != null ? execution.getStatus() : "FAILED");

                incapablesBatchRunning.set(false);
            }

        } else {
            log.info("Incapables Batch Schedular flag stopped");
        }

        runFlagIncapables = false;
    }
    @Scheduled(cron = "${spring.batch.schedular.leDeletionCron:0 */5 * * * *}")
    public void runLEDeletion() {
        log.info("LegalEntity Deletion Batch Schedular started:{}", LocalDateTime.now());
        JobExecution execution = null;
        String fileId = null;
        String fileName = null;

        if (!leDeletionBatchRunning.compareAndSet(false, true)) {
            log.info("LegalEntity Deletion Batch still running, skipping this run :{}", LocalDateTime.now());
            return;
        }
        if (runFlagLEDeletion) {
            try {
                log.info("LegalEntity Deletion Batch Schedular cron:{} started:{}", leDeletionCron, LocalDateTime.now());

                // Fetch file name from configuration
                fileName = cofaceLEDeleteFileName;
                String filePath = buildFilePath(fileName);


                Long totalRecords = CsvTotalRecordsParser.getTotalRecordsFromCsv(filePath);
                Long fileSize = CsvTotalRecordsParser.getFileSizeInBytes(filePath);

                fileId = fileIngestionService.registerFileIngestion(
                        fileName,
                        "COFACE_LE_DELETION",
                        "LE",
                        "COFACE_SYSTEM",
                        "DAILY",
                        totalRecords
                );

                JobParameters jobParameters = new JobParametersBuilder(jobExplorer)
                        .addLong("run.id", System.currentTimeMillis())
                        .addString("leDeletionFileId", fileId)
                        .addString("fileName", fileName)
                        .toJobParameters();

                // Mark file processing start
                fileIngestionService.markFileProcessingStart(fileId, String.valueOf(System.currentTimeMillis()));

                execution = jobLauncher.run(leDeletionJob, jobParameters);

                // Update file processing completion based on execution status
                if (execution.getStatus().toString().equals("COMPLETED")) {
                    log.info("LegalEntity Deletion Batch completed - fileId: {}", fileId);
                } else {
                    String errorMsg = "Batch execution failed with status: " + execution.getStatus();
                    log.error("LegalEntity Deletion Batch failed: {}", errorMsg);
                    fileIngestionService.markFileProcessingFailed(fileId, errorMsg);
                }
            } catch (Exception e) {
                log.error("Exception in LegalEntity Deletion Batch Schedular", e);

                // Log to DD_ERROR_LOG table
                try {
                    StringWriter sw = new StringWriter();
                    e.printStackTrace(new PrintWriter(sw));

                    errorLogDAO.logError(
                            RecordType.LEGAL_ENTITY_DELETION,
                            "BATCH_JOB",
                            null,
                            ErrorType.DATABASE_ERROR,
                            "Batch Scheduler Exception: " + e.getMessage(),
                            null,
                            fileName,
                            sw.toString(),
                            fileId
                    );
                } catch (Exception logEx) {
                    log.error("Failed to log batch scheduler error", logEx);
                }

                // Mark file as failed
                if (fileId != null) {
                    try {
                        fileIngestionService.markFileProcessingFailed(fileId, "Exception in batch scheduler: " + e.getMessage());
                    } catch (Exception fileEx) {
                        log.error("Failed to mark file as failed", fileEx);
                    }
                }
            } finally {
                if (execution != null) {
                    log.info("LegalEntity Deletion Batch Schedular finished name:{} status:{}", leDeletionJob.getName(), execution.getStatus());
                } else {
                    log.info("LegalEntity Deletion Batch Schedular failed name:{}", leDeletionJob.getName());
                }
                leDeletionBatchRunning.set(false);
            }
        } else {
            log.info("LegalEntity Deletion Batch Schedular flag stopped");
        }
        runFlagLEDeletion = false;
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
