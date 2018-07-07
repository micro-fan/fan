from fan.contrib.django import VARS
from tipsi_tools.logging import JSFormatter


class SpanFormatter(JSFormatter):  # TODO: [TRACING] Vars for sanic
    def process_log_record(self, rec):
        rec.update(VARS)
        return super().process_log_record(rec)
