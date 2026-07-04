import gen_thrift.common.ttypes as common

from ai.chronon import model


def test_model_partition_interval_sets_output_table_info():
    m = model.Model(version="v1", partition_interval="3h")

    table_info = m.metaData.executionInfo.outputTableInfo
    assert table_info.partitionColumn == "ds"
    assert table_info.partitionFormat == "yyyy-MM-dd-HH-mm"
    assert table_info.partitionInterval == common.Window(length=3, timeUnit=common.TimeUnit.HOURS)


def test_model_partition_offset_without_interval_assumes_daily_grid():
    m = model.Model(version="v1", partition_offset="1h")

    table_info = m.metaData.executionInfo.outputTableInfo
    assert table_info.partitionColumn == "ds"
    assert table_info.partitionFormat == "yyyy-MM-dd-HH-mm"
    assert table_info.partitionInterval == common.Window(length=1, timeUnit=common.TimeUnit.DAYS)
    assert table_info.partitionOffset == common.Window(length=1, timeUnit=common.TimeUnit.HOURS)
