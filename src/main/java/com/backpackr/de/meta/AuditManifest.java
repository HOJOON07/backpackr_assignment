package com.backpackr.de.meta;

public record AuditManifest(
        String runId,
        String status,
        String inputPath,
        String outputPath,
        String targetStartDate,
        String targetEndDate,
        Long rowCount,
        String startedAt,
        String finishedAt,
        String errorMessage
) {

    public static AuditManifest running(
            String runId,
            String inputPath,
            String outputPath,
            String targetStartDate,
            String targetEndDate,
            String startedAt) {
        return new AuditManifest(runId, "RUNNING", inputPath, outputPath,
                targetStartDate, targetEndDate, null, startedAt, null, null);
    }

    public AuditManifest succeeded(long rows, String finishedAt) {
        return new AuditManifest(runId, "SUCCESS", inputPath, outputPath,
                targetStartDate, targetEndDate, rows, startedAt, finishedAt, null);
    }

    public AuditManifest failed(String error, String finishedAt) {
        return new AuditManifest(runId, "FAILED", inputPath, outputPath,
                targetStartDate, targetEndDate, rowCount, startedAt, finishedAt, error);
    }
}
