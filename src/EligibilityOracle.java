import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
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
import java.util.Set;
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

        try {
        RuleSet rules = RuleSet.load(rulesPath);
        String idField = options.getOrDefault("id-field", "id");
        int limit = parseIntOption(options.get("limit"), -1);
        AuditResult result = audit(inputPath, rules, idField, limit);
            String report = format.equals("json") ? renderJson(result) : renderText(result);
            if (outputPath == null) {
                System.out.println(report);
            } else {
                Files.writeString(Path.of(outputPath), report, StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("Group Scholar Eligibility Oracle");
        System.out.println("Usage: java -cp src EligibilityOracle --input <file.csv> --rules <rules.txt> [--format text|json] [--output report.txt] [--id-field field] [--limit N]");
        System.out.println("Options:");
        System.out.println("  --input   Path to applicant intake CSV");
        System.out.println("  --rules   Path to eligibility rules file");
        System.out.println("  --format  text (default) or json");
        System.out.println("  --output  Optional output file path");
        System.out.println("  --id-field Field name to use for applicant identifiers (default: id)");
        System.out.println("  --limit   Limit number of ineligible applicants listed (default: no limit)");
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("--help") || arg.equals("-h")) {
                options.put("help", "true");
                return options;
            }
            if (arg.startsWith("--") && i + 1 < args.length) {
                options.put(arg.substring(2), args[i + 1]);
                i++;
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
        Map<String, Integer> headerIndex = new HashMap<>();
        for (int i = 0; i < headers.size(); i++) {
            headerIndex.put(normalize(headers.get(i)), i);
        }

        AuditResult result = new AuditResult();
        result.totalRows = Math.max(0, lines.size() - 1);
        result.failureLimit = limit;
        result.idField = normalize(idField);

        for (int i = 1; i < lines.size(); i++) {
            List<String> row = parseCsvLine(lines.get(i));
            Map<String, String> rowMap = new HashMap<>();
            for (int c = 0; c < headers.size(); c++) {
                String key = normalize(headers.get(c));
                String value = c < row.size() ? row.get(c).trim() : "";
                rowMap.put(key, value);
            }

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

            String id = rowMap.getOrDefault(result.idField, "row-" + i);
            if (reasons.isEmpty()) {
                result.eligible++;
            } else {
                result.ineligible++;
                if (result.failureLimit < 0 || result.failures.size() < result.failureLimit) {
                    result.failures.add(new FailureRecord(id, reasons));
                } else {
                    result.failuresTruncated = true;
                }
                for (String reason : reasons) {
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
    }

    private static class FailureRecord {
        String id;
        List<String> reasons;

        FailureRecord(String id, List<String> reasons) {
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
                }
            }
            rules.conditionalRequirements.addAll(conditionalLookup.values());
            rules.requiredFields.replaceAll(EligibilityOracle::normalize);
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
