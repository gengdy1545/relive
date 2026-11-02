package org.relive;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.Strictness;

import java.util.List;

/** Online requests contain explicit source relationships, never an offline Trace. */
public final class Protocol {
    public static final int MAX_BYTES = 8 * 1024 * 1024;
    public static final Gson JSON = new GsonBuilder().setStrictness(Strictness.STRICT)
            .disableHtmlEscaping().serializeNulls().create();

    private Protocol() {}

    public record Mutation(String index, String key, String value, boolean delete, String identity) {
        public Mutation(String index, String key, String value, boolean delete) {
            this(index, key, value, delete, key);
        }
        public Mutation {
            name(index, false);
            hex(key, false, 131072);
            if (identity == null) identity = key;
            hex(identity, false, 131072);
            if (delete) {
                if (value != null) throw new IllegalArgumentException("Deletion must not supply a value");
            } else {
                hex(value, true, 131072);
            }
        }
    }

    public record Commit(String parent, String id, List<Mutation> changes, Publication publication) {
        public Commit(String parent, String id, List<Mutation> changes) { this(parent, id, changes, null); }
        public Commit {
            name(parent, true);
            name(id, false);
            if (id.equals(parent)) throw new IllegalArgumentException("Commit cannot be its own parent");
            if (changes == null || changes.size() > 10000)
                throw new IllegalArgumentException("A commit requires a changes list of at most 10000 members");
            changes = List.copyOf(changes);
            if (publication != null && (!parent.equals(publication.parent()) || !id.equals(publication.id())
                    || changes.size() != publication.writes()))
                throw new IllegalArgumentException("Publication does not match its commit");
        }
    }

    public record Stage(String transaction, long operation, String id, List<Mutation> changes) {
        public Stage {
            name(transaction, false);
            name(id, false);
            if (operation < 1 || changes == null || changes.isEmpty() || changes.size() > 10000)
                throw new IllegalArgumentException("Invalid staged operation");
            changes = List.copyOf(changes);
        }
    }

    public record Publication(String transaction, String parent, String id, long operations, long writes) {
        public Publication {
            name(transaction, false); name(parent, true); name(id, false);
            if (id.equals(parent) || operations < 0 || writes < 0 || writes > 10000)
                throw new IllegalArgumentException("Invalid transaction publication");
        }
    }

    public record Progress(long operation) {}
    public record Binding(String binding) {
        public Binding {
            if (binding == null || binding.isBlank() || binding.length() > 4096)
                throw new IllegalArgumentException("Invalid index binding");
        }
    }
    public record BindingInfo(String binding, String identity, String head, String location) {}

    public record Scan(String index, String lower, boolean lowerInclusive,
                       String upper, boolean upperInclusive, boolean descending, long limit) {
        public Scan {
            name(index, false);
            if (lower != null) hex(lower, false, 131072);
            if (upper != null) hex(upper, false, 131072);
            if (lower != null && upper != null && lower.compareTo(upper) > 0)
                throw new IllegalArgumentException("Lower bound exceeds upper bound");
            if (limit < 0) throw new IllegalArgumentException("Limit must be nonnegative; zero is unbounded");
        }
    }

    public record Read(String at, List<Scan> scans, String transaction, long operation, String own,
                       String combine) {
        public Read(String at, List<Scan> scans) { this(at, scans, null, 0, null, "none"); }
        public Read {
            if (at != null) name(at, true);
            if (scans == null || scans.isEmpty() || scans.size() > 32)
                throw new IllegalArgumentException("A read requires 1..32 scans");
            scans = List.copyOf(scans);
            if (combine == null) combine = "none";
            if (!List.of("none", "union", "intersection").contains(combine)
                    || !combine.equals("none") && scans.size() < 2)
                throw new IllegalArgumentException("Invalid scan combination");
            if (transaction == null) {
                if (operation != 0 || own != null) throw new IllegalArgumentException("Local read needs a transaction");
            } else {
                name(transaction, false);
                if (operation < 1) throw new IllegalArgumentException("Invalid read operation");
                if (own != null) name(own, false);
            }
        }
    }

    public record Head(String id) {}
    public record Entry(String key, String value, String commit, String identity) {
        public Entry(String key, String value, String commit) { this(key, value, commit, key); }
    }
    public record Results(String at, List<List<Entry>> scans) {}
    public record Error(String message) {}

    static void name(String value, boolean emptyAllowed) {
        if (value == null || (!emptyAllowed && value.isEmpty()) || value.length() > 128
                || !value.chars().allMatch(c -> c >= 33 && c <= 126))
            throw new IllegalArgumentException("Identifiers require 1..128 printable ASCII characters");
    }

    private static void hex(String value, boolean emptyAllowed, int maximum) {
        if (value == null || (!emptyAllowed && value.isEmpty()) || value.length() > maximum
                || (value.length() & 1) != 0
                || !value.chars().allMatch(c -> c >= '0' && c <= '9' || c >= 'a' && c <= 'f'))
            throw new IllegalArgumentException("Keys and values require bounded lowercase hexadecimal bytes");
    }
}
