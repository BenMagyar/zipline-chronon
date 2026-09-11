"""Shared parsing and formatting for CLI partition date values."""

import datetime

PARTITION_DS_FORMATS = (
    ("%Y-%m-%d", "%Y-%m-%d"),
    ("%Y/%m/%d", "%Y-%m-%d"),
    ("%Y-%m-%d-%H", "%Y-%m-%d-%H-00"),
    ("%Y-%m-%d-%H-%M", "%Y-%m-%d-%H-%M"),
    ("%Y-%m-%d-%H:%M", "%Y-%m-%d-%H-%M"),
    ("%Y-%m-%d %H", "%Y-%m-%d-%H-00"),
    ("%Y-%m-%d %H:%M", "%Y-%m-%d-%H-%M"),
    ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d-%H-%M"),
    ("%Y/%m/%d %H", "%Y-%m-%d-%H-00"),
    ("%Y/%m/%d %H:%M", "%Y-%m-%d-%H-%M"),
    ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d-%H-%M"),
    ("%Y-%m-%dT%H", "%Y-%m-%d-%H-00"),
    ("%Y-%m-%dT%H:%M", "%Y-%m-%d-%H-%M"),
    ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d-%H-%M"),
)
PARTITION_DS_FORMAT_HELP = (
    "YYYY-MM-DD, YYYY-MM-DD-HH, YYYY-MM-DD-HH-mm, ISO, or space-separated datetime"
)


def _validate_partition_ds_precision(parsed, value):
    if parsed.second != 0 or parsed.microsecond != 0:
        raise ValueError(f"'{value}' must be aligned to minute precision")


def _parse_partition_ds_string(value):
    raw = str(value).strip()
    for input_format, output_format in PARTITION_DS_FORMATS:
        try:
            parsed = datetime.datetime.strptime(raw, input_format)
        except ValueError:
            continue
        _validate_partition_ds_precision(parsed, value)
        return parsed, output_format
    raise ValueError(
        f"'{value}' does not match any supported date format: {PARTITION_DS_FORMAT_HELP}"
    )


def parse_partition_ds(value):
    """Parse a supported partition date or datetime at minute precision."""
    if isinstance(value, datetime.datetime):
        _validate_partition_ds_precision(value, value)
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time.min)
    return _parse_partition_ds_string(value)[0]


def format_partition_ds(value):
    """Normalize a supported partition value to the Hub wire format."""
    if isinstance(value, (datetime.datetime, datetime.date)):
        parsed = parse_partition_ds(value)
        output_format = (
            "%Y-%m-%d-%H-%M" if parsed.time() != datetime.time.min else "%Y-%m-%d"
        )
        return parsed.strftime(output_format)
    parsed, output_format = _parse_partition_ds_string(value)
    return parsed.strftime(output_format)


def is_day_partition_value(value):
    """Return whether a value uses a date-only input format."""
    if isinstance(value, datetime.datetime):
        return value.time() == datetime.time.min
    if isinstance(value, datetime.date):
        return True
    try:
        _, output_format = _parse_partition_ds_string(value)
        return output_format == "%Y-%m-%d"
    except ValueError:
        return False


def partition_ds_to_epoch_millis(value):
    """Convert a supported partition value to UTC epoch milliseconds."""
    parsed = parse_partition_ds(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    else:
        parsed = parsed.astimezone(datetime.timezone.utc)
    return int(parsed.timestamp() * 1000)


def format_hub_partition(value, default_value):
    """Format typed Hub client values while preserving string inputs."""
    if value is None:
        return default_value
    if isinstance(value, (datetime.datetime, datetime.date)):
        return format_partition_ds(value)
    return str(value)
