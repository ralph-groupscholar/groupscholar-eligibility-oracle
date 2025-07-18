import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class EligibilityOracle {
    public static void main(String[] args) {
        Map<String, String> options = parseArgs(args);
        if (options.containsKey("help") || !options.containsKey("input") || !options.containsKey("rules")) {
            printUsage();
            System.exit(0);
        }

        Path inputPath = Path.of(options.get("input"));
        Path rulesPath = Path.of(options.get("rules"));
        String format = options.getOrDefault("format", "text").toLowerCase(Locale.ROOT);
        String outputPath = options.get("output");
        boolean logDb = options.containsKey("log-db");
        String runName = options.get("run-name");
        String segmentField = options.get("segment-field");
        int reviewLimit = parseIntOption(options.get("review-limit"), -1);

        try {
            RuleSet rules = RuleSet.load(rulesPath);
            String idField = options.getOrDefault("id-field", "id");
            int limit = parseIntOption(options.get("limit"), -1);
            AuditResult result = audit(inputPath, rules, idField, limit, segmentField, reviewLimit);
            result.runName = runName == null ? "" : runName;
            result.inputPath = inputPath.toString();
            result.rulesPath = rulesPath.toString();
            String report = format.equals("json") ? renderJson(result) : renderText(result);
            if (outputPath == null) {
                System.out.println(report);
            } else {
                Files.writeString(Path.of(outputPath), report, StandardCharsets.UTF_8);
            }
            if (logDb) {
                logToDatabase(result, inputPath, rulesPath, runName);
            }
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("Group Scholar Eligibility Oracle");
        System.out.println("Usage: java -cp src EligibilityOracle --input <file.csv> --rules <rules.txt> [--format text|json] [--output report.txt] [--id-field field] [--limit N] [--segment-field field] [--review-limit N] [--log-db] [--run-name name]");
        System.out.println("Options:");
        System.out.println("  --input   Path to applicant intake CSV");
        System.out.println("  --rules   Path to eligibility rules file");
        System.out.println("  --format  text (default) or json");
        System.out.println("  --output  Optional output file path");
        System.out.println("  --id-field Field name to use for applicant identifiers (default: id)");
        System.out.println("  --limit   Limit number of ineligible applicants listed (default: no limit)");
        System.out.println("  --segment-field Field to summarize eligibility breakdowns (ex: status)");
        System.out.println("  --review-limit Limit number of review-flagged applicants listed (default: no limit)");
        System.out.println("  --log-db  Write audit summary + failures to the Postgres analytics schema");
        System.out.println("  --run-name Optional label to store alongside the audit run");
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("--help") || arg.equals("-h")) {
                options.put("help", "true");
                return options;
            }
            if (arg.startsWith("--")) {
                String key = arg.substring(2);
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    options.put(key, args[i + 1]);
                    i++;
                } else {
                    options.put(key, "true");
                }
            }
        }
        return options;
    }

    private static AuditResult audit(Path inputPath, RuleSet rules, String idField, int limit, String segmentField, int reviewLimit) throws IOException {
        List<String> lines = Files.readAllLines(inputPath, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new IOException("Input CSV is empty.");
        }

        String headerLine = lines.get(0);
        List<String> headers = parseCsvLine(headerLine);
        AuditResult result = new AuditResult();
        result.totalRows = Math.max(0, lines.size() - 1);
        result.failureLimit = limit;
        result.reviewLimit = reviewLimit;
        result.idField = canonicalizeField(normalize(idField), rules);
        result.segmentField = segmentField == null ? "" : canonicalizeField(normalize(segmentField), rules);
        List<RowRecord> rows = new ArrayList<>();
        Map<String, Map<String, List<RowRecord>>> uniqueLookup = new LinkedHashMap<>();
        List<String> trackedFields = buildTrackedFields(rules);
        for (String field : trackedFields) {
            result.missingFieldCounts.put(field, 0);
        }

        for (int i = 1; i < lines.size(); i++) {
            List<String> row = parseCsvLine(lines.get(i));
            Map<String, String> rowMap = new HashMap<>();
            for (int c = 0; c < headers.size(); c++) {
                String key = normalize(headers.get(c));
                String value = c < row.size() ? row.get(c).trim() : "";
                rowMap.put(key, value);
            }
            applyAliases(rowMap, rules);
            for (String field : trackedFields) {
                String value = rowMap.getOrDefault(field, "");
                if (value.isBlank()) {
                    result.missingFieldCounts.put(field, result.missingFieldCounts.get(field) + 1);
                }
            }

            String id = rowMap.getOrDefault(result.idField, "row-" + i);
            List<String> reasons = new ArrayList<>();
            List<String> reviewReasons = new ArrayList<>();
            List<String> warnings = new ArrayList<>();
            for (String required : rules.requiredFields) {
                String value = rowMap.getOrDefault(required, "");
                if (value.isBlank()) {
                    reasons.add("missing:" + required);
                }
            }

            for (ConditionalRequirement requirement : rules.conditionalRequirements) {
                String value = normalizeValue(rowMap.getOrDefault(requirement.conditionField, ""));
                if (value.equals(requirement.conditionValue)) {
                    for (String needed : requirement.requiredFields) {
                        String requiredValue = rowMap.getOrDefault(needed, "");
                        if (requiredValue.isBlank()) {
                            reasons.add("missing_if:" + requirement.conditionField + "=" + requirement.conditionValue + ":" + needed);
                        }
                    }
                }
            }

            for (AnyRequirement requirement : rules.anyRequirements) {
                boolean hasAny = false;
                for (String field : requirement.fields) {
                    String value = rowMap.getOrDefault(field, "");
                    if (!value.isBlank()) {
                        hasAny = true;
                        break;
                    }
                }
                if (!hasAny) {
                    reasons.add("missing_any:" + requirement.name);
                }
            }

            for (String reviewField : rules.reviewMissingFields) {
                String value = rowMap.getOrDefault(reviewField, "");
                if (value.isBlank()) {
                    reviewReasons.add("review_missing:" + reviewField);
                }
            }

            for (ReviewCondition condition : rules.reviewConditions) {
                String value = normalizeValue(rowMap.getOrDefault(condition.conditionField, ""));
                if (value.equals(condition.conditionValue)) {
                    for (String reason : condition.reasons) {
                        reviewReasons.add("review_flag:" + reason);
                    }
                }
            }

            for (Map.Entry<String, NumericRange> entry : rules.numericRanges.entrySet()) {
                String field = entry.getKey();
                String value = rowMap.getOrDefault(field, "");
                if (value.isBlank()) {
                    continue;
                }
                try {
                    double numeric = Double.parseDouble(value);
                    if (numeric < entry.getValue().min || numeric > entry.getValue().max) {
                        reasons.add("out_of_range:" + field);
                    }
                } catch (NumberFormatException e) {
                    reasons.add("invalid_number:" + field);
                }
            }

            for (Map.Entry<String, Set<String>> entry : rules.allowedValues.entrySet()) {
                String field = entry.getKey();
                String value = normalizeValue(rowMap.getOrDefault(field, ""));
                if (value.isBlank()) {
                    continue;
                }
                if (!entry.getValue().contains(value)) {
                    reasons.add("disallowed:" + field);
                }
            }

            for (Map.Entry<String, Set<String>> entry : rules.disallowedValues.entrySet()) {
                String field = entry.getKey();
                String value = normalizeValue(rowMap.getOrDefault(field, ""));
                if (value.isBlank()) {
                    continue;
                }
                if (entry.getValue().contains(value)) {
                    reasons.add("blocked:" + field);
                }
            }

            for (Map.Entry<String, DateRange> entry : rules.dateRanges.entrySet()) {
                String field = entry.getKey();
                String value = rowMap.getOrDefault(field, "");
                if (value.isBlank()) {
                    continue;
                }
                try {
                    LocalDate date = LocalDate.parse(value);
                    if (date.isBefore(entry.getValue().earliest) || date.isAfter(entry.getValue().latest)) {
                        reasons.add("out_of_range:" + field);
                    }
                } catch (DateTimeParseException e) {
                    reasons.add("invalid_date:" + field);
                }
            }

            for (Map.Entry<String, Pattern> entry : rules.patternRules.entrySet()) {
                String field = entry.getKey();
                String value = rowMap.getOrDefault(field, "");
                if (value.isBlank()) {
                    continue;
                }
                if (!entry.getValue().matcher(value).matches()) {
                    reasons.add("invalid_pattern:" + field);
                }
            }

            for (String required : rules.warnRequiredFields) {
                String value = rowMap.getOrDefault(required, "");
                if (value.isBlank()) {
                    warnings.add("warn_missing:" + required);
                }
            }

            for (ConditionalRequirement requirement : rules.warnConditionalRequirements) {
                String value = normalizeValue(rowMap.getOrDefault(requirement.conditionField, ""));
                if (value.equals(requirement.conditionValue)) {
                    for (String needed : requirement.requiredFields) {
                        String requiredValue = rowMap.getOrDefault(needed, "");
                        if (requiredValue.isBlank()) {
                            warnings.add("warn_missing_if:" + requirement.conditionField + "=" + requirement.conditionValue + ":" + needed);
                        }
                    }
                }
            }

            for (AnyRequirement requirement : rules.warnAnyRequirements) {
                boolean hasAny = false;
                for (String field : requirement.fields) {
                    String value = rowMap.getOrDefault(field, "");
                    if (!value.isBlank()) {
                        hasAny = true;
                        break;
                    }
                }
                if (!hasAny) {
                    warnings.add("warn_missing_any:" + requirement.name);
                }
            }

            for (Map.Entry<String, NumericRange> entry : rules.warnNumericRanges.entrySet()) {
                String field = entry.getKey();
                String value = rowMap.getOrDefault(field, "");
                if (value.isBlank()) {
                    continue;
                }
                try {
                    double numeric = Double.parseDouble(value);
                    if (numeric < entry.getValue().min || numeric > entry.getValue().max) {
                        warnings.add("warn_out_of_range:" + field);
                    }
                } catch (NumberFormatException e) {
                    warnings.add("warn_invalid_number:" + field);
                }
            }

            for (Map.Entry<String, Set<String>> entry : rules.warnAllowedValues.entrySet()) {
                String field = entry.getKey();
                String value = normalizeValue(rowMap.getOrDefault(field, ""));
                if (value.isBlank()) {
                    continue;
                }
                if (!entry.getValue().contains(value)) {
                    warnings.add("warn_disallowed:" + field);
                }
            }

            for (Map.Entry<String, Set<String>> entry : rules.warnDisallowedValues.entrySet()) {
                String field = entry.getKey();
                String value = normalizeValue(rowMap.getOrDefault(field, ""));
                if (value.isBlank()) {
                    continue;
                }
                if (entry.getValue().contains(value)) {
                    warnings.add("warn_blocked:" + field);
                }
            }

            for (Map.Entry<String, DateRange> entry : rules.warnDateRanges.entrySet()) {
                String field = entry.getKey();
                String value = rowMap.getOrDefault(field, "");
                if (value.isBlank()) {
                    continue;
                }
                try {
                    LocalDate date = LocalDate.parse(value);
                    if (date.isBefore(entry.getValue().earliest) || date.isAfter(entry.getValue().latest)) {
                        warnings.add("warn_out_of_range:" + field);
                    }
                } catch (DateTimeParseException e) {
                    warnings.add("warn_invalid_date:" + field);
                }
            }

            for (Map.Entry<String, Pattern> entry : rules.warnPatternRules.entrySet()) {
                String field = entry.getKey();
                String value = rowMap.getOrDefault(field, "");
                if (value.isBlank()) {
                    continue;
                }
                if (!entry.getValue().matcher(value).matches()) {
                    warnings.add("warn_invalid_pattern:" + field);
                }
            }

            RowRecord record = new RowRecord(id, reasons, reviewReasons, warnings);
            rows.add(record);

            if (!result.segmentField.isBlank()) {
                String rawSegmentValue = rowMap.getOrDefault(result.segmentField, "").trim();
                String segmentValue = rawSegmentValue.isBlank() ? "missing" : normalizeValue(rawSegmentValue);
                SegmentStats stats = result.segmentStats.computeIfAbsent(segmentValue, key -> new SegmentStats(segmentValue));
                stats.total++;
                if (reasons.isEmpty()) {
                    stats.eligible++;
                } else {
                    stats.ineligible++;
                }
            }

            for (String uniqueField : rules.uniqueFields) {
                String value = rowMap.getOrDefault(uniqueField, "").trim();
                if (value.isBlank()) {
                    continue;
                }
                uniqueLookup
                        .computeIfAbsent(uniqueField, key -> new LinkedHashMap<>())
                        .computeIfAbsent(value, key -> new ArrayList<>())
                        .add(record);
            }
        }

        for (Map.Entry<String, Map<String, List<RowRecord>>> fieldEntry : uniqueLookup.entrySet()) {
            String field = fieldEntry.getKey();
            for (Map.Entry<String, List<RowRecord>> valueEntry : fieldEntry.getValue().entrySet()) {
                List<RowRecord> matches = valueEntry.getValue();
                if (matches.size() > 1) {
                    for (RowRecord record : matches) {
                        record.reasons.add("duplicate:" + field);
                    }
                }
            }
        }

        for (RowRecord record : rows) {
            if (record.reasons.isEmpty()) {
                result.eligible++;
            } else {
                result.ineligible++;
                if (result.failureLimit < 0 || result.failures.size() < result.failureLimit) {
                    result.failures.add(new FailureRecord(record.id, record.reasons));
                } else {
                    result.failuresTruncated = true;
                }
                for (String reason : record.reasons) {
                    result.reasonCounts.put(reason, result.reasonCounts.getOrDefault(reason, 0) + 1);
                    String category = reason.split(":", 2)[0];
                    result.reasonCategoryCounts.put(category, result.reasonCategoryCounts.getOrDefault(category, 0) + 1);
                }
            }
            if (!record.warningReasons.isEmpty()) {
                result.warningApplicants++;
                for (String warning : record.warningReasons) {
                    result.warningCounts.put(warning, result.warningCounts.getOrDefault(warning, 0) + 1);
                    String category = warning.split(":", 2)[0];
                    result.warningCategoryCounts.put(category, result.warningCategoryCounts.getOrDefault(category, 0) + 1);
                }
            }
            if (!record.reviewReasons.isEmpty()) {
                result.reviewCount++;
                if (result.reviewLimit < 0 || result.reviews.size() < result.reviewLimit) {
                    result.reviews.add(new ReviewRecord(record.id, record.reviewReasons));
                } else {
                    result.reviewsTruncated = true;
                }
                for (String reason : record.reviewReasons) {
                    result.reviewCounts.put(reason, result.reviewCounts.getOrDefault(reason, 0) + 1);
                }
            }
        }

        return result;
    }

    private static void applyAliases(Map<String, String> rowMap, RuleSet rules) {
        if (rules.aliases.isEmpty()) {
            return;
        }
        for (Map.Entry<String, List<String>> entry : rules.aliases.entrySet()) {
            String canonical = entry.getKey();
            String current = rowMap.getOrDefault(canonical, "");
            if (!current.isBlank()) {
                continue;
            }
            for (String alias : entry.getValue()) {
                String value = rowMap.getOrDefault(alias, "");
                if (!value.isBlank()) {
                    rowMap.put(canonical, value);
                    break;
                }
            }
        }
    }

    private static String canonicalizeField(String field, RuleSet rules) {
        if (field == null || field.isBlank()) {
            return field;
        }
        String canonical = rules.aliasToCanonical.get(field);
        return canonical == null ? field : canonical;
    }

    private static List<String> parseCsvLine(String line) {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (ch == ',' && !inQuotes) {
                values.add(current.toString());
                current.setLength(0);
            } else {
                current.append(ch);
            }
        }
        values.add(current.toString());
        return values;
    }

    private static String normalize(String value) {
        return value.trim().toLowerCase(Locale.ROOT).replace(" ", "_");
    }

    private static String normalizeValue(String value) {
        return value.trim().toLowerCase(Locale.ROOT).replace(" ", "_");
    }

    private static String renderText(AuditResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("Eligibility Audit Summary\n");
        if (!result.runName.isBlank()) {
            sb.append("Run name: ").append(result.runName).append("\n");
        }
        if (!result.inputPath.isBlank()) {
            sb.append("Input: ").append(result.inputPath).append("\n");
        }
        if (!result.rulesPath.isBlank()) {
            sb.append("Rules: ").append(result.rulesPath).append("\n");
        }
        sb.append("Total applicants: ").append(result.totalRows).append("\n");
        sb.append("Eligible: ").append(result.eligible).append(" (").append(formatRate(result.eligible, result.totalRows)).append(")\n");
        sb.append("Ineligible: ").append(result.ineligible).append(" (").append(formatRate(result.ineligible, result.totalRows)).append(")\n\n");
        if (!result.warningCounts.isEmpty()) {
            sb.append("Applicants with warnings: ").append(result.warningApplicants)
                    .append(" (").append(formatRate(result.warningApplicants, result.totalRows)).append(")\n\n");
        }

        if (!result.reasonCategoryCounts.isEmpty()) {
            sb.append("Reason categories:\n");
            result.reasonCategoryCounts.entrySet().stream()
                    .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue()))
                    .forEach(entry -> sb.append("- ").append(entry.getKey())
                            .append(": ").append(entry.getValue()).append("\n"));
            sb.append("\n");
        }

        if (!result.warningCategoryCounts.isEmpty()) {
            sb.append("Warning categories:\n");
            result.warningCategoryCounts.entrySet().stream()
                    .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue()))
                    .forEach(entry -> sb.append("- ").append(entry.getKey())
                            .append(": ").append(entry.getValue()).append("\n"));
            sb.append("\n");
        }

        if (!result.reasonCounts.isEmpty()) {
            sb.append("Top ineligibility reasons:\n");
            result.reasonCounts.entrySet().stream()
                    .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue()))
                    .forEach(entry -> sb.append("- ").append(entry.getKey())
                            .append(": ").append(entry.getValue()).append("\n"));
            sb.append("\n");
        }

        if (!result.warningCounts.isEmpty()) {
            sb.append("Top warnings:\n");
            result.warningCounts.entrySet().stream()
                    .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue()))
                    .forEach(entry -> sb.append("- ").append(entry.getKey())
                            .append(": ").append(entry.getValue()).append("\n"));
            sb.append("\n");
        }

        if (result.reviewCount > 0) {
            sb.append("Review flags: ").append(result.reviewCount)
                    .append(" (").append(formatRate(result.reviewCount, result.totalRows)).append(")\n");
            if (!result.reviewCounts.isEmpty()) {
                sb.append("Top review flags:\n");
                result.reviewCounts.entrySet().stream()
                        .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue()))
                        .forEach(entry -> sb.append("- ").append(entry.getKey())
                                .append(": ").append(entry.getValue()).append("\n"));
                sb.append("\n");
            }
        }

        if (!result.missingFieldCounts.isEmpty()) {
            List<Map.Entry<String, Integer>> missingEntries = result.missingFieldCounts.entrySet().stream()
                    .filter(entry -> entry.getValue() > 0)
                    .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue()))
                    .toList();
            if (!missingEntries.isEmpty()) {
                sb.append("Field completeness (missing values):\n");
                for (Map.Entry<String, Integer> entry : missingEntries) {
                    sb.append("- ").append(entry.getKey())
                            .append(": ").append(entry.getValue())
                            .append(" (").append(formatRate(entry.getValue(), result.totalRows)).append(")\n");
                }
                sb.append("\n");
            }
        }

        if (!result.segmentField.isBlank() && !result.segmentStats.isEmpty()) {
            sb.append("Segment breakdown (field: ").append(result.segmentField).append("):\n");
            result.segmentStats.values().stream()
                    .sorted((a, b) -> Integer.compare(b.total, a.total))
                    .forEach(stat -> sb.append("- ").append(stat.value)
                            .append(": total ").append(stat.total)
                            .append(" | eligible ").append(stat.eligible)
                            .append(" (").append(formatRate(stat.eligible, stat.total)).append(")")
                            .append(" | ineligible ").append(stat.ineligible)
                            .append(" (").append(formatRate(stat.ineligible, stat.total)).append(")")
                            .append("\n"));
            sb.append("\n");
        }

        if (!result.failures.isEmpty()) {
            sb.append("Ineligible applicants:");
            if (result.failureLimit >= 0) {
                sb.append(" (showing ").append(result.failures.size());
                if (result.failuresTruncated) {
                    sb.append(" of ").append(result.ineligible);
                }
                sb.append(")");
            }
            sb.append("\n");
            for (FailureRecord record : result.failures) {
                sb.append("- ").append(record.id).append(": ")
                        .append(String.join(", ", record.reasons)).append("\n");
            }
            if (result.failuresTruncated) {
                sb.append("... truncated\n");
            }
        }

        if (!result.reviews.isEmpty()) {
            sb.append("Review-flagged applicants:");
            if (result.reviewLimit >= 0) {
                sb.append(" (showing ").append(result.reviews.size());
                if (result.reviewsTruncated) {
                    sb.append(" of ").append(result.reviewCount);
                }
                sb.append(")");
            }
            sb.append("\n");
            for (ReviewRecord record : result.reviews) {
                sb.append("- ").append(record.id).append(": ")
                        .append(String.join(", ", record.reasons)).append("\n");
            }
            if (result.reviewsTruncated) {
                sb.append("... truncated\n");
            }
        }

        return sb.toString();
    }

    private static String renderJson(AuditResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"runName\": ").append(result.runName.isBlank() ? "null" : "\"" + escapeJson(result.runName) + "\"").append(",\n");
        sb.append("  \"inputPath\": ").append(result.inputPath.isBlank() ? "null" : "\"" + escapeJson(result.inputPath) + "\"").append(",\n");
        sb.append("  \"rulesPath\": ").append(result.rulesPath.isBlank() ? "null" : "\"" + escapeJson(result.rulesPath) + "\"").append(",\n");
        sb.append("  \"totalApplicants\": ").append(result.totalRows).append(",\n");
        sb.append("  \"eligible\": ").append(result.eligible).append(",\n");
        sb.append("  \"eligibleRate\": ").append(formatRateValue(result.eligible, result.totalRows)).append(",\n");
        sb.append("  \"ineligible\": ").append(result.ineligible).append(",\n");
        sb.append("  \"ineligibleRate\": ").append(formatRateValue(result.ineligible, result.totalRows)).append(",\n");
        sb.append("  \"warningApplicants\": ").append(result.warningApplicants).append(",\n");
        sb.append("  \"warningRate\": ").append(formatRateValue(result.warningApplicants, result.totalRows)).append(",\n");
        sb.append("  \"idField\": \"").append(escapeJson(result.idField)).append("\",\n");
        sb.append("  \"failureLimit\": ").append(result.failureLimit).append(",\n");
        sb.append("  \"failuresTruncated\": ").append(result.failuresTruncated).append(",\n");
        sb.append("  \"reviewCount\": ").append(result.reviewCount).append(",\n");
        sb.append("  \"reviewRate\": ").append(formatRateValue(result.reviewCount, result.totalRows)).append(",\n");
        sb.append("  \"reviewLimit\": ").append(result.reviewLimit).append(",\n");
        sb.append("  \"reviewsTruncated\": ").append(result.reviewsTruncated).append(",\n");
        sb.append("  \"segmentField\": ").append(result.segmentField.isBlank() ? "null" : "\"" + escapeJson(result.segmentField) + "\"").append(",\n");
        sb.append("  \"reasonCategories\": {");
        if (!result.reasonCategoryCounts.isEmpty()) {
            sb.append("\n");
            int catIdx = 0;
            for (Map.Entry<String, Integer> entry : result.reasonCategoryCounts.entrySet()) {
                sb.append("    \"").append(escapeJson(entry.getKey())).append("\": ").append(entry.getValue());
                catIdx++;
                sb.append(catIdx < result.reasonCategoryCounts.size() ? ",\n" : "\n");
            }
            sb.append("  ");
        }
        sb.append("},\n");
        sb.append("  \"warningCategories\": {");
        if (!result.warningCategoryCounts.isEmpty()) {
            sb.append("\n");
            int catIdx = 0;
            for (Map.Entry<String, Integer> entry : result.warningCategoryCounts.entrySet()) {
                sb.append("    \"").append(escapeJson(entry.getKey())).append("\": ").append(entry.getValue());
                catIdx++;
                sb.append(catIdx < result.warningCategoryCounts.size() ? ",\n" : "\n");
            }
            sb.append("  ");
        }
        sb.append("},\n");
        sb.append("  \"reasonCounts\": {");
        if (!result.reasonCounts.isEmpty()) {
            sb.append("\n");
            int idx = 0;
            for (Map.Entry<String, Integer> entry : result.reasonCounts.entrySet()) {
                sb.append("    \"").append(escapeJson(entry.getKey())).append("\": ").append(entry.getValue());
                idx++;
                sb.append(idx < result.reasonCounts.size() ? ",\n" : "\n");
            }
            sb.append("  ");
        }
        sb.append("},\n");
        sb.append("  \"warningCounts\": {");
        if (!result.warningCounts.isEmpty()) {
            sb.append("\n");
            int idx = 0;
            for (Map.Entry<String, Integer> entry : result.warningCounts.entrySet()) {
                sb.append("    \"").append(escapeJson(entry.getKey())).append("\": ").append(entry.getValue());
                idx++;
                sb.append(idx < result.warningCounts.size() ? ",\n" : "\n");
            }
            sb.append("  ");
        }
        sb.append("},\n");
        sb.append("  \"missingFieldCounts\": {");
        if (!result.missingFieldCounts.isEmpty()) {
            sb.append("\n");
            int idx = 0;
            for (Map.Entry<String, Integer> entry : result.missingFieldCounts.entrySet()) {
                sb.append("    \"").append(escapeJson(entry.getKey())).append("\": ").append(entry.getValue());
                idx++;
                sb.append(idx < result.missingFieldCounts.size() ? ",\n" : "\n");
            }
            sb.append("  ");
        }
        sb.append("},\n");
        sb.append("  \"missingFieldRates\": {");
        if (!result.missingFieldCounts.isEmpty()) {
            sb.append("\n");
            int idx = 0;
            for (Map.Entry<String, Integer> entry : result.missingFieldCounts.entrySet()) {
                sb.append("    \"").append(escapeJson(entry.getKey())).append("\": ")
                        .append(formatRateValue(entry.getValue(), result.totalRows));
                idx++;
                sb.append(idx < result.missingFieldCounts.size() ? ",\n" : "\n");
            }
            sb.append("  ");
        }
        sb.append("},\n");
        sb.append("  \"failures\": [");
        if (!result.failures.isEmpty()) {
            sb.append("\n");
            for (int i = 0; i < result.failures.size(); i++) {
                FailureRecord record = result.failures.get(i);
                sb.append("    {\"id\": \"").append(escapeJson(record.id)).append("\", \"reasons\": [");
                for (int r = 0; r < record.reasons.size(); r++) {
                    sb.append("\"").append(escapeJson(record.reasons.get(r))).append("\"");
                    if (r + 1 < record.reasons.size()) {
                        sb.append(", ");
                    }
                }
                sb.append("]}");
                sb.append(i + 1 < result.failures.size() ? ",\n" : "\n");
            }
            sb.append("  ");
        }
        sb.append("],\n");
        sb.append("  \"reviewCounts\": {");
        if (!result.reviewCounts.isEmpty()) {
            sb.append("\n");
            int idx = 0;
            for (Map.Entry<String, Integer> entry : result.reviewCounts.entrySet()) {
                sb.append("    \"").append(escapeJson(entry.getKey())).append("\": ").append(entry.getValue());
                idx++;
                sb.append(idx < result.reviewCounts.size() ? ",\n" : "\n");
            }
            sb.append("  ");
        }
        sb.append("},\n");
        sb.append("  \"reviews\": [");
        if (!result.reviews.isEmpty()) {
            sb.append("\n");
            for (int i = 0; i < result.reviews.size(); i++) {
                ReviewRecord record = result.reviews.get(i);
                sb.append("    {\"id\": \"").append(escapeJson(record.id)).append("\", \"reasons\": [");
                for (int r = 0; r < record.reasons.size(); r++) {
                    sb.append("\"").append(escapeJson(record.reasons.get(r))).append("\"");
                    if (r + 1 < record.reasons.size()) {
                        sb.append(", ");
                    }
                }
                sb.append("]}");
                sb.append(i + 1 < result.reviews.size() ? ",\n" : "\n");
            }
            sb.append("  ");
        }
        sb.append("],\n");
        sb.append("  \"segments\": [");
        if (!result.segmentStats.isEmpty()) {
            sb.append("\n");
            List<SegmentStats> stats = new ArrayList<>(result.segmentStats.values());
            stats.sort((a, b) -> Integer.compare(b.total, a.total));
            for (int i = 0; i < stats.size(); i++) {
                SegmentStats stat = stats.get(i);
                sb.append("    {\"value\": \"").append(escapeJson(stat.value)).append("\",")
                        .append(" \"total\": ").append(stat.total).append(",")
                        .append(" \"eligible\": ").append(stat.eligible).append(",")
                        .append(" \"eligibleRate\": ").append(formatRateValue(stat.eligible, stat.total)).append(",")
                        .append(" \"ineligible\": ").append(stat.ineligible).append(",")
                        .append(" \"ineligibleRate\": ").append(formatRateValue(stat.ineligible, stat.total)).append("}");
                sb.append(i + 1 < stats.size() ? ",\n" : "\n");
            }
            sb.append("  ");
        }
        sb.append("]\n");
        sb.append("}\n");
        return sb.toString();
    }

    private static void logToDatabase(AuditResult result, Path inputPath, Path rulesPath, String runName) {
        DbConfig config = DbConfig.fromEnv();
        if (!config.enabled) {
            System.err.println("DB logging requested but ELIGIBILITY_DB_URL is not set.");
            return;
        }

        Properties props = new Properties();
        if (config.user != null && !config.user.isBlank()) {
            props.setProperty("user", config.user);
        }
        if (config.password != null && !config.password.isBlank()) {
            props.setProperty("password", config.password);
        }

        try (Connection conn = DriverManager.getConnection(config.url, props)) {
            conn.setAutoCommit(false);
            ensureSchema(conn, config.schema);
            long runId = insertAuditRun(conn, config.schema, result, inputPath, rulesPath, runName);
            insertReasonCounts(conn, config.schema, runId, result.reasonCounts, "audit_reason_counts", "reason");
            insertReasonCounts(conn, config.schema, runId, result.reasonCategoryCounts, "audit_reason_categories", "category");
            insertReasonCounts(conn, config.schema, runId, result.warningCounts, "audit_warning_counts", "warning");
            insertReasonCounts(conn, config.schema, runId, result.warningCategoryCounts, "audit_warning_categories", "category");
            insertReasonCounts(conn, config.schema, runId, result.reviewCounts, "audit_review_counts", "reason");
            insertFieldCompleteness(conn, config.schema, runId, result);
            insertFailures(conn, config.schema, runId, result.failures);
            insertReviews(conn, config.schema, runId, result.reviews);
            insertSegments(conn, config.schema, runId, result);
            conn.commit();
            System.err.println("Logged audit to DB (run_id=" + runId + ").");
        } catch (SQLException e) {
            System.err.println("DB logging failed: " + e.getMessage());
        }
    }

    private static void ensureSchema(Connection conn, String schema) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement("CREATE SCHEMA IF NOT EXISTS " + schema)) {
            stmt.execute();
        }
        String runsSql = "CREATE TABLE IF NOT EXISTS " + schema + ".audit_runs (" +
                "id BIGSERIAL PRIMARY KEY," +
                "run_at TIMESTAMPTZ NOT NULL," +
                "run_name TEXT," +
                "input_file TEXT," +
                "rules_file TEXT," +
                "total_applicants INT NOT NULL," +
                "eligible INT NOT NULL," +
                "ineligible INT NOT NULL," +
                "eligible_rate NUMERIC(6,4) NOT NULL," +
                "ineligible_rate NUMERIC(6,4) NOT NULL," +
                "warning_applicants INT," +
                "warning_rate NUMERIC(6,4)," +
                "review_count INT," +
                "review_rate NUMERIC(6,4)," +
                "id_field TEXT NOT NULL," +
                "failure_limit INT," +
                "failures_truncated BOOLEAN NOT NULL," +
                "review_limit INT," +
                "reviews_truncated BOOLEAN" +
                ")";
        String reasonSql = "CREATE TABLE IF NOT EXISTS " + schema + ".audit_reason_counts (" +
                "run_id BIGINT REFERENCES " + schema + ".audit_runs(id) ON DELETE CASCADE," +
                "reason TEXT NOT NULL," +
                "count INT NOT NULL" +
                ")";
        String categorySql = "CREATE TABLE IF NOT EXISTS " + schema + ".audit_reason_categories (" +
                "run_id BIGINT REFERENCES " + schema + ".audit_runs(id) ON DELETE CASCADE," +
                "category TEXT NOT NULL," +
                "count INT NOT NULL" +
                ")";
        String warningSql = "CREATE TABLE IF NOT EXISTS " + schema + ".audit_warning_counts (" +
                "run_id BIGINT REFERENCES " + schema + ".audit_runs(id) ON DELETE CASCADE," +
                "warning TEXT NOT NULL," +
                "count INT NOT NULL" +
                ")";
        String warningCategorySql = "CREATE TABLE IF NOT EXISTS " + schema + ".audit_warning_categories (" +
                "run_id BIGINT REFERENCES " + schema + ".audit_runs(id) ON DELETE CASCADE," +
                "category TEXT NOT NULL," +
                "count INT NOT NULL" +
                ")";
        String reviewSql = "CREATE TABLE IF NOT EXISTS " + schema + ".audit_review_counts (" +
                "run_id BIGINT REFERENCES " + schema + ".audit_runs(id) ON DELETE CASCADE," +
                "reason TEXT NOT NULL," +
                "count INT NOT NULL" +
                ")";
        String completenessSql = "CREATE TABLE IF NOT EXISTS " + schema + ".audit_field_completeness (" +
                "run_id BIGINT REFERENCES " + schema + ".audit_runs(id) ON DELETE CASCADE," +
                "field_name TEXT NOT NULL," +
                "missing_count INT NOT NULL," +
                "missing_rate NUMERIC(6,4) NOT NULL" +
                ")";
        String failureSql = "CREATE TABLE IF NOT EXISTS " + schema + ".audit_failures (" +
                "run_id BIGINT REFERENCES " + schema + ".audit_runs(id) ON DELETE CASCADE," +
                "applicant_id TEXT NOT NULL," +
                "reasons TEXT[] NOT NULL" +
                ")";
        String reviewFlagSql = "CREATE TABLE IF NOT EXISTS " + schema + ".audit_reviews (" +
                "run_id BIGINT REFERENCES " + schema + ".audit_runs(id) ON DELETE CASCADE," +
                "applicant_id TEXT NOT NULL," +
                "reasons TEXT[] NOT NULL" +
                ")";
        String segmentSql = "CREATE TABLE IF NOT EXISTS " + schema + ".audit_segments (" +
                "run_id BIGINT REFERENCES " + schema + ".audit_runs(id) ON DELETE CASCADE," +
                "segment_field TEXT NOT NULL," +
                "segment_value TEXT NOT NULL," +
                "total INT NOT NULL," +
                "eligible INT NOT NULL," +
                "ineligible INT NOT NULL," +
                "eligible_rate NUMERIC(6,4) NOT NULL," +
                "ineligible_rate NUMERIC(6,4) NOT NULL" +
                ")";
        try (PreparedStatement stmt = conn.prepareStatement(runsSql)) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement("ALTER TABLE " + schema + ".audit_runs ADD COLUMN IF NOT EXISTS warning_applicants INT")) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement("ALTER TABLE " + schema + ".audit_runs ADD COLUMN IF NOT EXISTS warning_rate NUMERIC(6,4)")) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement("ALTER TABLE " + schema + ".audit_runs ADD COLUMN IF NOT EXISTS review_count INT")) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement("ALTER TABLE " + schema + ".audit_runs ADD COLUMN IF NOT EXISTS review_rate NUMERIC(6,4)")) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement("ALTER TABLE " + schema + ".audit_runs ADD COLUMN IF NOT EXISTS review_limit INT")) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement("ALTER TABLE " + schema + ".audit_runs ADD COLUMN IF NOT EXISTS reviews_truncated BOOLEAN")) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement(reasonSql)) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement(categorySql)) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement(warningSql)) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement(warningCategorySql)) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement(reviewSql)) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement(completenessSql)) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement(failureSql)) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement(reviewFlagSql)) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement(segmentSql)) {
            stmt.execute();
        }
    }

    private static long insertAuditRun(Connection conn, String schema, AuditResult result, Path inputPath, Path rulesPath, String runName) throws SQLException {
        double eligibleRate = result.totalRows == 0 ? 0.0 : (result.eligible * 1.0) / result.totalRows;
        double ineligibleRate = result.totalRows == 0 ? 0.0 : (result.ineligible * 1.0) / result.totalRows;
        double warningRate = result.totalRows == 0 ? 0.0 : (result.warningApplicants * 1.0) / result.totalRows;
        double reviewRate = result.totalRows == 0 ? 0.0 : (result.reviewCount * 1.0) / result.totalRows;
        String sql = "INSERT INTO " + schema + ".audit_runs " +
                "(run_at, run_name, input_file, rules_file, total_applicants, eligible, ineligible, eligible_rate, ineligible_rate, warning_applicants, warning_rate, review_count, review_rate, id_field, failure_limit, failures_truncated, review_limit, reviews_truncated) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, OffsetDateTime.now());
            stmt.setString(2, runName);
            stmt.setString(3, inputPath.toString());
            stmt.setString(4, rulesPath.toString());
            stmt.setInt(5, result.totalRows);
            stmt.setInt(6, result.eligible);
            stmt.setInt(7, result.ineligible);
            stmt.setBigDecimal(8, java.math.BigDecimal.valueOf(eligibleRate));
            stmt.setBigDecimal(9, java.math.BigDecimal.valueOf(ineligibleRate));
            stmt.setInt(10, result.warningApplicants);
            stmt.setBigDecimal(11, java.math.BigDecimal.valueOf(warningRate));
            stmt.setInt(12, result.reviewCount);
            stmt.setBigDecimal(13, java.math.BigDecimal.valueOf(reviewRate));
            stmt.setString(14, result.idField);
            if (result.failureLimit >= 0) {
                stmt.setInt(15, result.failureLimit);
            } else {
                stmt.setNull(15, java.sql.Types.INTEGER);
            }
            stmt.setBoolean(16, result.failuresTruncated);
            if (result.reviewLimit >= 0) {
                stmt.setInt(17, result.reviewLimit);
            } else {
                stmt.setNull(17, java.sql.Types.INTEGER);
            }
            stmt.setBoolean(18, result.reviewsTruncated);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }
        throw new SQLException("Failed to insert audit run row.");
    }

    private static void insertReasonCounts(Connection conn, String schema, long runId, Map<String, Integer> counts, String table, String keyColumn) throws SQLException {
        if (counts.isEmpty()) {
            return;
        }
        String sql = "INSERT INTO " + schema + "." + table + " (run_id, " + keyColumn + ", count) VALUES (?, ?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                stmt.setLong(1, runId);
                stmt.setString(2, entry.getKey());
                stmt.setInt(3, entry.getValue());
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
    }

    private static void insertFieldCompleteness(Connection conn, String schema, long runId, AuditResult result) throws SQLException {
        if (result.missingFieldCounts.isEmpty()) {
            return;
        }
        String sql = "INSERT INTO " + schema + ".audit_field_completeness (run_id, field_name, missing_count, missing_rate) VALUES (?, ?, ?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (Map.Entry<String, Integer> entry : result.missingFieldCounts.entrySet()) {
                stmt.setLong(1, runId);
                stmt.setString(2, entry.getKey());
                stmt.setInt(3, entry.getValue());
                double missingRate = result.totalRows == 0 ? 0.0 : (entry.getValue() * 1.0) / result.totalRows;
                stmt.setBigDecimal(4, java.math.BigDecimal.valueOf(missingRate));
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
    }

    private static void insertFailures(Connection conn, String schema, long runId, List<FailureRecord> failures) throws SQLException {
        if (failures.isEmpty()) {
            return;
        }
        String sql = "INSERT INTO " + schema + ".audit_failures (run_id, applicant_id, reasons) VALUES (?, ?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (FailureRecord record : failures) {
                stmt.setLong(1, runId);
                stmt.setString(2, record.id);
                Array reasonArray = conn.createArrayOf("text", record.reasons.toArray());
                stmt.setArray(3, reasonArray);
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
    }

    private static void insertReviews(Connection conn, String schema, long runId, List<ReviewRecord> reviews) throws SQLException {
        if (reviews.isEmpty()) {
            return;
        }
        String sql = "INSERT INTO " + schema + ".audit_reviews (run_id, applicant_id, reasons) VALUES (?, ?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (ReviewRecord record : reviews) {
                stmt.setLong(1, runId);
                stmt.setString(2, record.id);
                Array reasonArray = conn.createArrayOf("text", record.reasons.toArray());
                stmt.setArray(3, reasonArray);
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
    }

    private static void insertSegments(Connection conn, String schema, long runId, AuditResult result) throws SQLException {
        if (result.segmentField.isBlank() || result.segmentStats.isEmpty()) {
            return;
        }
        String sql = "INSERT INTO " + schema + ".audit_segments " +
                "(run_id, segment_field, segment_value, total, eligible, ineligible, eligible_rate, ineligible_rate) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (SegmentStats stat : result.segmentStats.values()) {
                double eligibleRate = stat.total == 0 ? 0.0 : (stat.eligible * 1.0) / stat.total;
                double ineligibleRate = stat.total == 0 ? 0.0 : (stat.ineligible * 1.0) / stat.total;
                stmt.setLong(1, runId);
                stmt.setString(2, result.segmentField);
                stmt.setString(3, stat.value);
                stmt.setInt(4, stat.total);
                stmt.setInt(5, stat.eligible);
                stmt.setInt(6, stat.ineligible);
                stmt.setBigDecimal(7, java.math.BigDecimal.valueOf(eligibleRate));
                stmt.setBigDecimal(8, java.math.BigDecimal.valueOf(ineligibleRate));
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
    }

    private static String escapeJson(String value) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default: sb.append(ch);
            }
        }
        return sb.toString();
    }

    private static class AuditResult {
        int totalRows = 0;
        int eligible = 0;
        int ineligible = 0;
        int warningApplicants = 0;
        int reviewCount = 0;
        Map<String, Integer> reasonCounts = new LinkedHashMap<>();
        Map<String, Integer> reasonCategoryCounts = new LinkedHashMap<>();
        Map<String, Integer> warningCounts = new LinkedHashMap<>();
        Map<String, Integer> warningCategoryCounts = new LinkedHashMap<>();
        Map<String, Integer> missingFieldCounts = new LinkedHashMap<>();
        Map<String, Integer> reviewCounts = new LinkedHashMap<>();
        List<FailureRecord> failures = new ArrayList<>();
        List<ReviewRecord> reviews = new ArrayList<>();
        int failureLimit = -1;
        boolean failuresTruncated = false;
        int reviewLimit = -1;
        boolean reviewsTruncated = false;
        String idField = "id";
        String runName = "";
        String inputPath = "";
        String rulesPath = "";
        String segmentField = "";
        Map<String, SegmentStats> segmentStats = new LinkedHashMap<>();
    }

    private static class SegmentStats {
        String value;
        int total;
        int eligible;
        int ineligible;

        SegmentStats(String value) {
            this.value = value;
        }
    }

    private static class DbConfig {
        boolean enabled;
        String url;
        String user;
        String password;
        String schema;

        static DbConfig fromEnv() {
            DbConfig config = new DbConfig();
            config.url = System.getenv("ELIGIBILITY_DB_URL");
            config.user = System.getenv("ELIGIBILITY_DB_USER");
            config.password = System.getenv("ELIGIBILITY_DB_PASSWORD");
            String schema = System.getenv("ELIGIBILITY_DB_SCHEMA");
            config.schema = (schema == null || schema.isBlank()) ? "eligibility_oracle" : schema.trim();
            config.enabled = config.url != null && !config.url.isBlank();
            return config;
        }
    }

    private static class FailureRecord {
        String id;
        List<String> reasons;

        FailureRecord(String id, List<String> reasons) {
            this.id = id;
            this.reasons = reasons;
        }
    }

    private static class RowRecord {
        String id;
        List<String> reasons;
        List<String> reviewReasons;
        List<String> warningReasons;

        RowRecord(String id, List<String> reasons, List<String> reviewReasons, List<String> warningReasons) {
            this.id = id;
            this.reasons = reasons;
            this.reviewReasons = reviewReasons;
            this.warningReasons = warningReasons;
        }
    }

    private static class ReviewRecord {
        String id;
        List<String> reasons;

        ReviewRecord(String id, List<String> reasons) {
            this.id = id;
            this.reasons = reasons;
        }
    }

    private static class NumericRange {
        double min;
        double max;

        NumericRange(double min, double max) {
            this.min = min;
            this.max = max;
        }
    }

    private static class DateRange {
        LocalDate earliest;
        LocalDate latest;

        DateRange(LocalDate earliest, LocalDate latest) {
            this.earliest = earliest;
            this.latest = latest;
        }
    }

    private static class RuleSet {
        List<String> requiredFields = new ArrayList<>();
        List<ConditionalRequirement> conditionalRequirements = new ArrayList<>();
        List<AnyRequirement> anyRequirements = new ArrayList<>();
        List<String> reviewMissingFields = new ArrayList<>();
        List<ReviewCondition> reviewConditions = new ArrayList<>();
        Map<String, NumericRange> numericRanges = new LinkedHashMap<>();
        Map<String, Set<String>> allowedValues = new LinkedHashMap<>();
        Map<String, Set<String>> disallowedValues = new LinkedHashMap<>();
        Map<String, DateRange> dateRanges = new LinkedHashMap<>();
        Map<String, Pattern> patternRules = new LinkedHashMap<>();
        List<String> uniqueFields = new ArrayList<>();
        List<String> warnRequiredFields = new ArrayList<>();
        List<ConditionalRequirement> warnConditionalRequirements = new ArrayList<>();
        List<AnyRequirement> warnAnyRequirements = new ArrayList<>();
        Map<String, NumericRange> warnNumericRanges = new LinkedHashMap<>();
        Map<String, Set<String>> warnAllowedValues = new LinkedHashMap<>();
        Map<String, Set<String>> warnDisallowedValues = new LinkedHashMap<>();
        Map<String, DateRange> warnDateRanges = new LinkedHashMap<>();
        Map<String, Pattern> warnPatternRules = new LinkedHashMap<>();
        Map<String, List<String>> aliases = new LinkedHashMap<>();
        Map<String, String> aliasToCanonical = new LinkedHashMap<>();

        static RuleSet load(Path path) throws IOException {
            RuleSet rules = new RuleSet();
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            String section = "";
            Map<String, ConditionalRequirement> conditionalLookup = new LinkedHashMap<>();
            Map<String, AnyRequirement> anyLookup = new LinkedHashMap<>();
            Map<String, ConditionalRequirement> warnConditionalLookup = new LinkedHashMap<>();
            Map<String, AnyRequirement> warnAnyLookup = new LinkedHashMap<>();
            Map<String, ReviewCondition> reviewLookup = new LinkedHashMap<>();
            for (String raw : lines) {
                String line = raw.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                if (line.startsWith("[") && line.endsWith("]")) {
                    section = line.substring(1, line.length() - 1).toLowerCase(Locale.ROOT);
                    continue;
                }
                String[] parts = line.split("=", 2);
                if (parts.length != 2) {
                    continue;
                }
                String key = parts[0].trim().toLowerCase(Locale.ROOT);
                String value = parts[1].trim();
                if (section.equals("required")) {
                    if (key.equals("fields")) {
                        rules.requiredFields = normalizeList(value);
                    }
                } else if (section.equals("review_missing")) {
                    if (key.equals("fields")) {
                        rules.reviewMissingFields = normalizeList(value);
                    }
                } else if (section.equals("warn_required")) {
                    if (key.equals("fields")) {
                        rules.warnRequiredFields = normalizeList(value);
                    }
                } else if (section.startsWith("require_if:")) {
                    if (key.equals("fields")) {
                        String condition = section.substring("require_if:".length());
                        String[] conditionParts = condition.split("=", 2);
                        if (conditionParts.length == 2) {
                            String conditionField = normalize(conditionParts[0]);
                            String conditionValue = normalizeValue(conditionParts[1]);
                            String conditionKey = conditionField + "=" + conditionValue;
                            ConditionalRequirement requirement = conditionalLookup.get(conditionKey);
                            if (requirement == null) {
                                requirement = new ConditionalRequirement(conditionField, conditionValue);
                                conditionalLookup.put(conditionKey, requirement);
                            }
                            requirement.requiredFields.addAll(normalizeList(value));
                        }
                    }
                } else if (section.startsWith("warn_if:")) {
                    if (key.equals("fields")) {
                        String condition = section.substring("warn_if:".length());
                        String[] conditionParts = condition.split("=", 2);
                        if (conditionParts.length == 2) {
                            String conditionField = normalize(conditionParts[0]);
                            String conditionValue = normalizeValue(conditionParts[1]);
                            String conditionKey = conditionField + "=" + conditionValue;
                            ConditionalRequirement requirement = warnConditionalLookup.get(conditionKey);
                            if (requirement == null) {
                                requirement = new ConditionalRequirement(conditionField, conditionValue);
                                warnConditionalLookup.put(conditionKey, requirement);
                            }
                            requirement.requiredFields.addAll(normalizeList(value));
                        }
                    }
                } else if (section.startsWith("review_if:")) {
                    if (key.equals("reasons")) {
                        String condition = section.substring("review_if:".length());
                        String[] conditionParts = condition.split("=", 2);
                        if (conditionParts.length == 2) {
                            String conditionField = normalize(conditionParts[0]);
                            String conditionValue = normalizeValue(conditionParts[1]);
                            String conditionKey = conditionField + "=" + conditionValue;
                            ReviewCondition requirement = reviewLookup.get(conditionKey);
                            if (requirement == null) {
                                requirement = new ReviewCondition(conditionField, conditionValue);
                                reviewLookup.put(conditionKey, requirement);
                            }
                            requirement.reasons.addAll(normalizeList(value));
                        }
                    }
                } else if (section.startsWith("require_any:")) {
                    if (key.equals("fields")) {
                        String name = normalizeValue(section.substring("require_any:".length()));
                        AnyRequirement requirement = anyLookup.get(name);
                        if (requirement == null) {
                            requirement = new AnyRequirement(name);
                            anyLookup.put(name, requirement);
                        }
                        requirement.fields.addAll(normalizeList(value));
                    }
                } else if (section.startsWith("warn_any:")) {
                    if (key.equals("fields")) {
                        String name = normalizeValue(section.substring("warn_any:".length()));
                        AnyRequirement requirement = warnAnyLookup.get(name);
                        if (requirement == null) {
                            requirement = new AnyRequirement(name);
                            warnAnyLookup.put(name, requirement);
                        }
                        requirement.fields.addAll(normalizeList(value));
                    }
                } else if (section.startsWith("range:")) {
                    String field = section.substring("range:".length());
                    double min = rules.numericRanges.containsKey(field) ? rules.numericRanges.get(field).min : Double.NEGATIVE_INFINITY;
                    double max = rules.numericRanges.containsKey(field) ? rules.numericRanges.get(field).max : Double.POSITIVE_INFINITY;
                    if (key.equals("min")) {
                        min = Double.parseDouble(value);
                    } else if (key.equals("max")) {
                        max = Double.parseDouble(value);
                    }
                    rules.numericRanges.put(field, new NumericRange(min, max));
                } else if (section.startsWith("warn_range:")) {
                    String field = section.substring("warn_range:".length());
                    double min = rules.warnNumericRanges.containsKey(field) ? rules.warnNumericRanges.get(field).min : Double.NEGATIVE_INFINITY;
                    double max = rules.warnNumericRanges.containsKey(field) ? rules.warnNumericRanges.get(field).max : Double.POSITIVE_INFINITY;
                    if (key.equals("min")) {
                        min = Double.parseDouble(value);
                    } else if (key.equals("max")) {
                        max = Double.parseDouble(value);
                    }
                    rules.warnNumericRanges.put(field, new NumericRange(min, max));
                } else if (section.startsWith("allowed:")) {
                    String field = section.substring("allowed:".length());
                    if (key.equals("values")) {
                        Set<String> values = new HashSet<>();
                        for (String entry : normalizeList(value)) {
                            values.add(entry.toLowerCase(Locale.ROOT));
                        }
                        rules.allowedValues.put(field, values);
                    }
                } else if (section.startsWith("warn_allowed:")) {
                    String field = section.substring("warn_allowed:".length());
                    if (key.equals("values")) {
                        Set<String> values = new HashSet<>();
                        for (String entry : normalizeList(value)) {
                            values.add(entry.toLowerCase(Locale.ROOT));
                        }
                        rules.warnAllowedValues.put(field, values);
                    }
                } else if (section.startsWith("disallowed:")) {
                    String field = section.substring("disallowed:".length());
                    if (key.equals("values")) {
                        Set<String> values = new HashSet<>();
                        for (String entry : normalizeList(value)) {
                            values.add(entry.toLowerCase(Locale.ROOT));
                        }
                        rules.disallowedValues.put(field, values);
                    }
                } else if (section.startsWith("warn_disallowed:")) {
                    String field = section.substring("warn_disallowed:".length());
                    if (key.equals("values")) {
                        Set<String> values = new HashSet<>();
                        for (String entry : normalizeList(value)) {
                            values.add(entry.toLowerCase(Locale.ROOT));
                        }
                        rules.warnDisallowedValues.put(field, values);
                    }
                } else if (section.startsWith("date:")) {
                    String field = section.substring("date:".length());
                    LocalDate earliest = rules.dateRanges.containsKey(field) ? rules.dateRanges.get(field).earliest : LocalDate.MIN;
                    LocalDate latest = rules.dateRanges.containsKey(field) ? rules.dateRanges.get(field).latest : LocalDate.MAX;
                    if (key.equals("earliest")) {
                        earliest = LocalDate.parse(value);
                    } else if (key.equals("latest")) {
                        latest = LocalDate.parse(value);
                    }
                    rules.dateRanges.put(field, new DateRange(earliest, latest));
                } else if (section.startsWith("warn_date:")) {
                    String field = section.substring("warn_date:".length());
                    LocalDate earliest = rules.warnDateRanges.containsKey(field) ? rules.warnDateRanges.get(field).earliest : LocalDate.MIN;
                    LocalDate latest = rules.warnDateRanges.containsKey(field) ? rules.warnDateRanges.get(field).latest : LocalDate.MAX;
                    if (key.equals("earliest")) {
                        earliest = LocalDate.parse(value);
                    } else if (key.equals("latest")) {
                        latest = LocalDate.parse(value);
                    }
                    rules.warnDateRanges.put(field, new DateRange(earliest, latest));
                } else if (section.startsWith("pattern:")) {
                    String field = section.substring("pattern:".length());
                    if (key.equals("regex")) {
                        try {
                            rules.patternRules.put(field, Pattern.compile(value));
                        } catch (PatternSyntaxException e) {
                            throw new IOException("Invalid regex for pattern:" + field + " -> " + e.getMessage(), e);
                        }
                    }
                } else if (section.startsWith("warn_pattern:")) {
                    String field = section.substring("warn_pattern:".length());
                    if (key.equals("regex")) {
                        try {
                            rules.warnPatternRules.put(field, Pattern.compile(value));
                        } catch (PatternSyntaxException e) {
                            throw new IOException("Invalid regex for warn_pattern:" + field + " -> " + e.getMessage(), e);
                        }
                    }
                } else if (section.equals("unique")) {
                    if (key.equals("fields")) {
                        rules.uniqueFields = normalizeList(value);
                    }
                } else if (section.equals("aliases")) {
                    String canonical = normalize(key);
                    List<String> aliasList = normalizeList(value);
                    if (!canonical.isBlank() && !aliasList.isEmpty()) {
                        rules.aliases.put(canonical, aliasList);
                        for (String alias : aliasList) {
                            rules.aliasToCanonical.put(alias, canonical);
                        }
                    }
                }
            }
            rules.conditionalRequirements.addAll(conditionalLookup.values());
            rules.anyRequirements.addAll(anyLookup.values());
            rules.warnConditionalRequirements.addAll(warnConditionalLookup.values());
            rules.warnAnyRequirements.addAll(warnAnyLookup.values());
            rules.reviewConditions.addAll(reviewLookup.values());
            rules.requiredFields.replaceAll(EligibilityOracle::normalize);
            rules.uniqueFields.replaceAll(EligibilityOracle::normalize);
            rules.warnRequiredFields.replaceAll(EligibilityOracle::normalize);
            rules.reviewMissingFields.replaceAll(EligibilityOracle::normalize);
            for (AnyRequirement requirement : rules.anyRequirements) {
                requirement.fields.replaceAll(EligibilityOracle::normalize);
            }
            for (AnyRequirement requirement : rules.warnAnyRequirements) {
                requirement.fields.replaceAll(EligibilityOracle::normalize);
            }
            return rules;
        }

        private static List<String> normalizeList(String value) {
            if (value.isBlank()) {
                return Collections.emptyList();
            }
            String[] parts = value.split(",");
            List<String> results = new ArrayList<>();
            for (String part : parts) {
                results.add(normalizeValue(part));
            }
            return results;
        }
    }

    private static class ConditionalRequirement {
        String conditionField;
        String conditionValue;
        List<String> requiredFields = new ArrayList<>();

        ConditionalRequirement(String conditionField, String conditionValue) {
            this.conditionField = conditionField;
            this.conditionValue = conditionValue;
        }
    }

    private static class AnyRequirement {
        String name;
        List<String> fields = new ArrayList<>();

        AnyRequirement(String name) {
            this.name = name;
        }
    }

    private static class ReviewCondition {
        String conditionField;
        String conditionValue;
        List<String> reasons = new ArrayList<>();

        ReviewCondition(String conditionField, String conditionValue) {
            this.conditionField = conditionField;
            this.conditionValue = conditionValue;
        }
    }

    private static String formatRate(int count, int total) {
        if (total == 0) {
            return "0.00%";
        }
        double rate = (count * 100.0) / total;
        return String.format(Locale.ROOT, "%.2f%%", rate);
    }

    private static String formatRateValue(int count, int total) {
        if (total == 0) {
            return "0";
        }
        double rate = (count * 1.0) / total;
        return String.format(Locale.ROOT, "%.4f", rate);
    }

    private static int parseIntOption(String value, int fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static List<String> buildTrackedFields(RuleSet rules) {
        LinkedHashMap<String, Boolean> fields = new LinkedHashMap<>();
        addAll(fields, rules.requiredFields);
        addAll(fields, rules.reviewMissingFields);
        for (ConditionalRequirement requirement : rules.conditionalRequirements) {
            addAll(fields, Arrays.asList(requirement.conditionField));
            addAll(fields, requirement.requiredFields);
        }
        for (ReviewCondition condition : rules.reviewConditions) {
            addAll(fields, Arrays.asList(condition.conditionField));
        }
        for (AnyRequirement requirement : rules.anyRequirements) {
            addAll(fields, requirement.fields);
        }
        addAll(fields, rules.warnRequiredFields);
        for (ConditionalRequirement requirement : rules.warnConditionalRequirements) {
            addAll(fields, Arrays.asList(requirement.conditionField));
            addAll(fields, requirement.requiredFields);
        }
        for (AnyRequirement requirement : rules.warnAnyRequirements) {
            addAll(fields, requirement.fields);
        }
        addAll(fields, rules.numericRanges.keySet());
        addAll(fields, rules.allowedValues.keySet());
        addAll(fields, rules.disallowedValues.keySet());
        addAll(fields, rules.dateRanges.keySet());
        addAll(fields, rules.patternRules.keySet());
        addAll(fields, rules.warnNumericRanges.keySet());
        addAll(fields, rules.warnAllowedValues.keySet());
        addAll(fields, rules.warnDisallowedValues.keySet());
        addAll(fields, rules.warnDateRanges.keySet());
        addAll(fields, rules.warnPatternRules.keySet());
        addAll(fields, rules.uniqueFields);
        return new ArrayList<>(fields.keySet());
    }

    private static void addAll(Map<String, Boolean> target, Iterable<String> values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                target.put(value, Boolean.TRUE);
            }
        }
    }
}
