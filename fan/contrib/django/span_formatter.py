from fan.contrib.django import VARS
from tipsi_tools.tipsi_logging import JSFormatter


class SpanFormatter(JSFormatter):
    def process_log_record(self, rec):
        rec.update(VARS)
        return super().process_log_record(rec)
