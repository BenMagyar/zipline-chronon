package ai.chronon.online;

import ai.chronon.online.fetcher.Fetcher;

public class JavaGroupByStatusResponse {
    public String groupByName;
    public String batchEndDate;
    public Long batchEndTs;

    public JavaGroupByStatusResponse(String groupByName, String batchEndDate) {
        this(groupByName, batchEndDate, null);
    }

    public JavaGroupByStatusResponse(String groupByName, String batchEndDate, Long batchEndTs) {
        this.groupByName = groupByName;
        this.batchEndDate = batchEndDate;
        this.batchEndTs = batchEndTs;
    }

    public JavaGroupByStatusResponse(Fetcher.GroupByStatusResponse scalaResponse) {
        this.groupByName = scalaResponse.groupByName();
        this.batchEndDate = scalaResponse.batchEndDate();
        this.batchEndTs = scalaResponse.batchEndTs();
    }

    public Fetcher.GroupByStatusResponse toScala() {
        return new Fetcher.GroupByStatusResponse(groupByName, batchEndDate, batchEndTs);
    }
}
