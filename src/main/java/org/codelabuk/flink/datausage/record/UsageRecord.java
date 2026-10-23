package org.codelabuk.flink.datausage.record;

import java.time.Instant;
import java.util.Objects;

import static org.codelabuk.flink.datausage.source.UsageRecordGenerator.NUMBER_OF_ACCOUNTS_PER_INSTANCE;

public class UsageRecord {

    public Instant ts;
    public String account;
    public long byteUsed;

    public UsageRecord() {
    }

    public UsageRecord(final Instant ts, final String account, final long byteUsed) {
        this.ts = ts;
        this.account = account;
        this.byteUsed = byteUsed;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        UsageRecord that = (UsageRecord) o;
        return byteUsed == that.byteUsed && Objects.equals(ts, that.ts) && Objects.equals(account, that.account);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ts, account, byteUsed);
    }

    @Override
    public String toString() {
        return "UsageRecord{" +
                "ts=" + ts +
                ", account='" + account + '\'' +
                ", byteUsed=" + byteUsed +
                '}';
    }

    public static String accountForSubtaskAndIndex(int subtask, int index) {
        return String.format("%06d", (NUMBER_OF_ACCOUNTS_PER_INSTANCE * subtask) + index);
    }
}
