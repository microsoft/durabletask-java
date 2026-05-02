// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.exporthistory.client.ExportHistoryClient;
import com.microsoft.durabletask.exporthistory.client.ExportHistoryClients;
import com.microsoft.durabletask.exporthistory.client.ExportHistoryJobClient;
import com.microsoft.durabletask.exporthistory.exception.ExportJobNotFoundException;
import com.microsoft.durabletask.exporthistory.models.*;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REST controller for export history jobs. Matches the .NET ExportJobController endpoints.
 * <p>
 * POST   /export-jobs         — Create a new export job
 * GET    /export-jobs/{id}    — Get export job by ID
 * GET    /export-jobs/list    — List export jobs with optional filters
 * DELETE /export-jobs/{id}    — Delete an export job
 */
@RestController
@RequestMapping("/export-jobs")
public class ExportJobController {

    private static final Logger logger = Logger.getLogger(ExportJobController.class.getName());
    private final ExportHistoryClient exportClient;

    public ExportJobController() {
        this.exportClient = ExportHistoryClients.create(
                ExportHistorySample.client, ExportHistorySample.storageOptions);
    }

    @PostMapping
    public ResponseEntity<?> createJob(@RequestBody CreateExportJobRequest req) {
        try {
            ExportDestination dest = null;
            if (req.getContainer() != null && !req.getContainer().isEmpty()) {
                dest = new ExportDestination(req.getContainer(), req.getPrefix());
            }

            ExportJobCreationOptions opts = new ExportJobCreationOptions(
                    req.getJobId(), req.getMode(),
                    req.getCompletedTimeFrom(), req.getCompletedTimeTo(),
                    dest, req.getFormat(), req.getRuntimeStatus(),
                    req.getMaxInstancesPerBatch());

            ExportHistoryJobClient jobClient = exportClient.createJob(opts);
            ExportJobDescription desc = jobClient.describe();
            return ResponseEntity.status(HttpStatus.CREATED).body(desc);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to create export job", e);
            return ResponseEntity.status(500).body("Error: " + e.getMessage());
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getJob(@PathVariable String id) {
        try {
            return ResponseEntity.ok(exportClient.getJob(id));
        } catch (ExportJobNotFoundException e) {
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to get job: " + id, e);
            return ResponseEntity.status(500).body("Error: " + e.getMessage());
        }
    }

    @GetMapping("/list")
    public ResponseEntity<?> listJobs(
            @RequestParam(required = false) ExportJobStatus status,
            @RequestParam(required = false) String jobIdPrefix,
            @RequestParam(required = false) String createdFrom,
            @RequestParam(required = false) String createdTo,
            @RequestParam(required = false) Integer pageSize,
            @RequestParam(required = false) String continuationToken) {
        try {
            ExportJobQuery query = null;
            if (status != null || jobIdPrefix != null || createdFrom != null
                    || createdTo != null || pageSize != null || continuationToken != null) {
                query = new ExportJobQuery(status, jobIdPrefix,
                        createdFrom != null ? java.time.Instant.parse(createdFrom) : null,
                        createdTo != null ? java.time.Instant.parse(createdTo) : null,
                        pageSize, continuationToken);
            }

            List<ExportJobDescription> jobs = new ArrayList<>();
            for (ExportJobDescription job : exportClient.listJobs(query)) {
                jobs.add(job);
            }
            return ResponseEntity.ok(jobs);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to list jobs", e);
            return ResponseEntity.status(500).body("Error: " + e.getMessage());
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteJob(@PathVariable String id) {
        try {
            exportClient.getJobClient(id).delete();
            return ResponseEntity.noContent().build();
        } catch (ExportJobNotFoundException e) {
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to delete job: " + id, e);
            return ResponseEntity.status(500).body("Error: " + e.getMessage());
        }
    }
}
