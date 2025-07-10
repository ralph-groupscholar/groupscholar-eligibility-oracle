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

        try {
            RuleSet rules = RuleSet.load(rulesPath);
            String idField = options.getOrDefault("id-field", "id");
            int limit = parseIntOption(options.get("limit"), -1);
            AuditResult result = audit(inputPath, rules, idField, limit);
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
        System.out.println("Usage: java -cp src EligibilityOracle --input <file.csv> --rules <rules.txt> [--format text|json] [--output report.txt] [--id-field field] [--limit N] [--log-db] [--run-name name]");
        System.out.println("Options:");
        System.out.println("  --input   Path to applicant intake CSV");
        System.out.println("  --rules   Path to eligibility rules file");
        System.out.println("  --format  text (default) or json");
        System.out.println("  --output  Optional output file path");
        System.out.println("  --id-field Field name to use for applicant identifiers (default: id)");
        System.out.println("  --limit   Limit number of ineligible applicants listed (default: no limit)");
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

    private static AuditResult audit(Path inputPath, RuleSet rules, String idField, int limit) throws IOException {
        List<String> lines = Files.readAllLines(inputPath, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new IOException("Input CSV is empty.");
        }

        String headerLine = lines.get(0);
        List<String> headers = parseCsvLine(headerLine);
        AuditResult result = new AuditResult();
        result.totalRows = Math.max(0, lines.size() - 1);
        result.failureLimit = limit;
        result.idField = normalize(idField);
        List<RowRecord> rows = new ArrayList<>();
        Map<String, Map<String, List<RowRecord>>> uniqueLookup = new LinkedHashMap<>();

        for (int i = 1; i < lines.size(); i++) {
            List<String> row = parseCsvLine(lines.get(i));
            Map<String, String> rowMap = new HashMap<>();
            for (int c = 0; c < headers.size(); c++) {
                String key = normalize(headers.get(c));
                String value = c < row.size() ? row.get(c).trim() : "";
                rowMap.put(key, value);
            }

            String id = rowMap.getOrDefault(result.idField, "row-" + i);
            List<String> reasons = new ArrayList<>();
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
                String value = rowMap.getOrDefault(field, "");
                if (value.isBlank()) {
                    continue;
                }
                if (!entry.getValue().contains(value.toLowerCase(Locale.ROOT))) {
                    reasons.add("disallowed:" + field);
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

            RowRecord record = new RowRecord(id, reasons);
            rows.add(record);

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
        }

        return result;
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

        if (!result.reasonCategoryCounts.isEmpty()) {
            sb.append("Reason categories:\n");
            result.reasonCategoryCounts.entrySet().stream()
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
        sb.append("  \"idField\": \"").append(escapeJson(result.idField)).append("\",\n");
        sb.append("  \"failureLimit\": ").append(result.failureLimit).append(",\n");
        sb.append("  \"failuresTruncated\": ").append(result.failuresTruncated).append(",\n");
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
            insertFailures(conn, config.schema, runId, result.failures);
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
                "id_field TEXT NOT NULL," +
                "failure_limit INT," +
                "failures_truncated BOOLEAN NOT NULL" +
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
        String failureSql = "CREATE TABLE IF NOT EXISTS " + schema + ".audit_failures (" +
                "run_id BIGINT REFERENCES " + schema + ".audit_runs(id) ON DELETE CASCADE," +
                "applicant_id TEXT NOT NULL," +
                "reasons TEXT[] NOT NULL" +
                ")";
        try (PreparedStatement stmt = conn.prepareStatement(runsSql)) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement(reasonSql)) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement(categorySql)) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement(failureSql)) {
            stmt.execute();
        }
    }

    private static long insertAuditRun(Connection conn, String schema, AuditResult result, Path inputPath, Path rulesPath, String runName) throws SQLException {
        double eligibleRate = result.totalRows == 0 ? 0.0 : (result.eligible * 1.0) / result.totalRows;
        double ineligibleRate = result.totalRows == 0 ? 0.0 : (result.ineligible * 1.0) / result.totalRows;
        String sql = "INSERT INTO " + schema + ".audit_runs " +
                "(run_at, run_name, input_file, rules_file, total_applicants, eligible, ineligible, eligible_rate, ineligible_rate, id_field, failure_limit, failures_truncated) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id";
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
            stmt.setString(10, result.idField);
            if (result.failureLimit >= 0) {
                stmt.setInt(11, result.failureLimit);
            } else {
                stmt.setNull(11, java.sql.Types.INTEGER);
            }
            stmt.setBoolean(12, result.failuresTruncated);
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
        Map<String, Integer> reasonCounts = new LinkedHashMap<>();
        Map<String, Integer> reasonCategoryCounts = new LinkedHashMap<>();
        List<FailureRecord> failures = new ArrayList<>();
        int failureLimit = -1;
        boolean failuresTruncated = false;
        String idField = "id";
        String runName = "";
        String inputPath = "";
        String rulesPath = "";
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

        RowRecord(String id, List<String> reasons) {
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
        Map<String, NumericRange> numericRanges = new LinkedHashMap<>();
        Map<String, Set<String>> allowedValues = new LinkedHashMap<>();
        Map<String, DateRange> dateRanges = new LinkedHashMap<>();
        Map<String, Pattern> patternRules = new LinkedHashMap<>();
        List<String> uniqueFields = new ArrayList<>();

        static RuleSet load(Path path) throws IOException {
            RuleSet rules = new RuleSet();
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            String section = "";
            Map<String, ConditionalRequirement> conditionalLookup = new LinkedHashMap<>();
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
                } else if (section.startsWith("allowed:")) {
                    String field = section.substring("allowed:".length());
                    if (key.equals("values")) {
                        Set<String> values = new HashSet<>();
                        for (String entry : normalizeList(value)) {
                            values.add(entry.toLowerCase(Locale.ROOT));
                        }
                        rules.allowedValues.put(field, values);
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
                } else if (section.startsWith("pattern:")) {
                    String field = section.substring("pattern:".length());
                    if (key.equals("regex")) {
                        try {
                            rules.patternRules.put(field, Pattern.compile(value));
                        } catch (PatternSyntaxException e) {
                            throw new IOException("Invalid regex for pattern:" + field + " -> " + e.getMessage(), e);
                        }
                    }
                } else if (section.equals("unique")) {
                    if (key.equals("fields")) {
                        rules.uniqueFields = normalizeList(value);
                    }
                }
            }
            rules.conditionalRequirements.addAll(conditionalLookup.values());
            rules.requiredFields.replaceAll(EligibilityOracle::normalize);
            rules.uniqueFields.replaceAll(EligibilityOracle::normalize);
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
}
