"""Generated protocol buffer code."""

from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder

_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC, 6, 31, 1, "", "types.proto"
)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x0btypes.proto\x12\nprometheus"\xf8\x01\n\x0eMetricMetadata\x12\x33\n\x04type\x18\x01 \x01(\x0e\x32%.prometheus.MetricMetadata.MetricType\x12\x1a\n\x12metric_family_name\x18\x02 \x01(\t\x12\x0c\n\x04help\x18\x04 \x01(\t\x12\x0c\n\x04unit\x18\x05 \x01(\t"y\n\nMetricType\x12\x0b\n\x07UNKNOWN\x10\x00\x12\x0b\n\x07\x43OUNTER\x10\x01\x12\t\n\x05GAUGE\x10\x02\x12\r\n\tHISTOGRAM\x10\x03\x12\x12\n\x0eGAUGEHISTOGRAM\x10\x04\x12\x0b\n\x07SUMMARY\x10\x05\x12\x08\n\x04INFO\x10\x06\x12\x0c\n\x08STATESET\x10\x07"*\n\x06Sample\x12\r\n\x05value\x18\x01 \x01(\x01\x12\x11\n\ttimestamp\x18\x02 \x01(\x03"O\n\x08\x45xemplar\x12!\n\x06labels\x18\x01 \x03(\x0b\x32\x11.prometheus.Label\x12\r\n\x05value\x18\x02 \x01(\x01\x12\x11\n\ttimestamp\x18\x03 \x01(\x03"\x92\x04\n\tHistogram\x12\x13\n\tcount_int\x18\x01 \x01(\x04H\x00\x12\x15\n\x0b\x63ount_float\x18\x02 \x01(\x01H\x00\x12\x0b\n\x03sum\x18\x03 \x01(\x01\x12\x0e\n\x06schema\x18\x04 \x01(\x11\x12\x16\n\x0ezero_threshold\x18\x05 \x01(\x01\x12\x18\n\x0ezero_count_int\x18\x06 \x01(\x04H\x01\x12\x1a\n\x10zero_count_float\x18\x07 \x01(\x01H\x01\x12.\n\x0enegative_spans\x18\x08 \x03(\x0b\x32\x16.prometheus.BucketSpan\x12\x17\n\x0fnegative_deltas\x18\t \x03(\x12\x12\x17\n\x0fnegative_counts\x18\n \x03(\x01\x12.\n\x0epositive_spans\x18\x0b \x03(\x0b\x32\x16.prometheus.BucketSpan\x12\x17\n\x0fpositive_deltas\x18\x0c \x03(\x12\x12\x17\n\x0fpositive_counts\x18\r \x03(\x01\x12\x33\n\nreset_hint\x18\x0e \x01(\x0e\x32\x1f.prometheus.Histogram.ResetHint\x12\x11\n\ttimestamp\x18\x0f \x01(\x03\x12\x15\n\rcustom_values\x18\x10 \x03(\x01"4\n\tResetHint\x12\x0b\n\x07UNKNOWN\x10\x00\x12\x07\n\x03YES\x10\x01\x12\x06\n\x02NO\x10\x02\x12\t\n\x05GAUGE\x10\x03\x42\x07\n\x05\x63ountB\x0c\n\nzero_count",\n\nBucketSpan\x12\x0e\n\x06offset\x18\x01 \x01(\x11\x12\x0e\n\x06length\x18\x02 \x01(\r"\xa8\x01\n\nTimeSeries\x12!\n\x06labels\x18\x01 \x03(\x0b\x32\x11.prometheus.Label\x12#\n\x07samples\x18\x02 \x03(\x0b\x32\x12.prometheus.Sample\x12\'\n\texemplars\x18\x03 \x03(\x0b\x32\x14.prometheus.Exemplar\x12)\n\nhistograms\x18\x04 \x03(\x0b\x32\x15.prometheus.Histogram"$\n\x05Label\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t"+\n\x06Labels\x12!\n\x06labels\x18\x01 \x03(\x0b\x32\x11.prometheus.Label"\x82\x01\n\x0cLabelMatcher\x12+\n\x04type\x18\x01 \x01(\x0e\x32\x1d.prometheus.LabelMatcher.Type\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\r\n\x05value\x18\x03 \x01(\t"(\n\x04Type\x12\x06\n\x02\x45Q\x10\x00\x12\x07\n\x03NEQ\x10\x01\x12\x06\n\x02RE\x10\x02\x12\x07\n\x03NRE\x10\x03"|\n\tReadHints\x12\x0f\n\x07step_ms\x18\x01 \x01(\x03\x12\x0c\n\x04\x66unc\x18\x02 \x01(\t\x12\x10\n\x08start_ms\x18\x03 \x01(\x03\x12\x0e\n\x06\x65nd_ms\x18\x04 \x01(\x03\x12\x10\n\x08grouping\x18\x05 \x03(\t\x12\n\n\x02\x62y\x18\x06 \x01(\x08\x12\x10\n\x08range_ms\x18\x07 \x01(\x03"\xaf\x01\n\x05\x43hunk\x12\x13\n\x0bmin_time_ms\x18\x01 \x01(\x03\x12\x13\n\x0bmax_time_ms\x18\x02 \x01(\x03\x12(\n\x04type\x18\x03 \x01(\x0e\x32\x1a.prometheus.Chunk.Encoding\x12\x0c\n\x04\x64\x61ta\x18\x04 \x01(\x0c"D\n\x08\x45ncoding\x12\x0b\n\x07UNKNOWN\x10\x00\x12\x07\n\x03XOR\x10\x01\x12\r\n\tHISTOGRAM\x10\x02\x12\x13\n\x0f\x46LOAT_HISTOGRAM\x10\x03"U\n\rChunkedSeries\x12!\n\x06labels\x18\x01 \x03(\x0b\x32\x11.prometheus.Label\x12!\n\x06\x63hunks\x18\x02 \x03(\x0b\x32\x11.prometheus.Chunkb\x06proto3'
)

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "types_pb2", _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    DESCRIPTOR._loaded_options = None
    _globals["_METRICMETADATA"]._serialized_start = 28
    _globals["_METRICMETADATA"]._serialized_end = 276
    _globals["_METRICMETADATA_METRICTYPE"]._serialized_start = 155
    _globals["_METRICMETADATA_METRICTYPE"]._serialized_end = 276
    _globals["_SAMPLE"]._serialized_start = 278
    _globals["_SAMPLE"]._serialized_end = 320
    _globals["_EXEMPLAR"]._serialized_start = 322
    _globals["_EXEMPLAR"]._serialized_end = 401
    _globals["_HISTOGRAM"]._serialized_start = 404
    _globals["_HISTOGRAM"]._serialized_end = 934
    _globals["_HISTOGRAM_RESETHINT"]._serialized_start = 859
    _globals["_HISTOGRAM_RESETHINT"]._serialized_end = 911
    _globals["_BUCKETSPAN"]._serialized_start = 936
    _globals["_BUCKETSPAN"]._serialized_end = 980
    _globals["_TIMESERIES"]._serialized_start = 983
    _globals["_TIMESERIES"]._serialized_end = 1151
    _globals["_LABEL"]._serialized_start = 1153
    _globals["_LABEL"]._serialized_end = 1189
    _globals["_LABELS"]._serialized_start = 1191
    _globals["_LABELS"]._serialized_end = 1234
    _globals["_LABELMATCHER"]._serialized_start = 1237
    _globals["_LABELMATCHER"]._serialized_end = 1367
    _globals["_LABELMATCHER_TYPE"]._serialized_start = 1327
    _globals["_LABELMATCHER_TYPE"]._serialized_end = 1367
    _globals["_READHINTS"]._serialized_start = 1369
    _globals["_READHINTS"]._serialized_end = 1493
    _globals["_CHUNK"]._serialized_start = 1496
    _globals["_CHUNK"]._serialized_end = 1671
    _globals["_CHUNK_ENCODING"]._serialized_start = 1603
    _globals["_CHUNK_ENCODING"]._serialized_end = 1671
    _globals["_CHUNKEDSERIES"]._serialized_start = 1673
    _globals["_CHUNKEDSERIES"]._serialized_end = 1758
