package org.relive;

import org.junit.Test;
import java.util.List;
import static org.junit.Assert.assertThrows;

public class PublicationProbeTest {
    @Test public void independentCheckRejectsWrongValuesVersionsAndPartialIndexes() {
        var rows = List.of(new Protocol.Entry("01", "aa", "a"));
        PublicationProbe.check(new Protocol.Results("a", List.of(rows, rows)), "a", rows);
        assertThrows(AssertionError.class, () -> PublicationProbe.check(
                new Protocol.Results("a", List.of(rows, List.of())), "a", rows));
        assertThrows(AssertionError.class, () -> PublicationProbe.check(
                new Protocol.Results("b", List.of(rows, rows)), "a", rows));
        var wrong = List.of(new Protocol.Entry("01", "bb", "a"));
        assertThrows(AssertionError.class, () -> PublicationProbe.check(
                new Protocol.Results("a", List.of(wrong, wrong)), "a", rows));
    }
}
