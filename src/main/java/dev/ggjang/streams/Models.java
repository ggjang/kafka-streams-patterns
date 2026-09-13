package dev.ggjang.streams;

public final class Models {
    private Models() {}

    public enum Severity { NORMAL, WARNING, CRITICAL, NO_RULE }

    public record Reading(double temperature) {
        public Reading {
            if (!Double.isFinite(temperature)) {
                throw new IllegalArgumentException("temperature must be finite");
            }
        }
    }

    public record Rule(double warning, double critical) {
        public Rule {
            if (!Double.isFinite(warning) || !Double.isFinite(critical) || warning >= critical) {
                throw new IllegalArgumentException("finite thresholds with warning < critical are required");
            }
        }

        public Severity evaluate(double temperature) {
            if (temperature >= critical) return Severity.CRITICAL;
            if (temperature >= warning) return Severity.WARNING;
            return Severity.NORMAL;
        }
    }

    public record EvaluatedReading(double temperature, Severity severity) {}
}
