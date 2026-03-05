from prometheus_fastapi_instrumentator import Instrumentator


def get_instrumentator():
    return Instrumentator(
        should_group_status_codes=True,
        should_ignore_untemplated=False,
        should_instrument_requests_inprogress=True,
        inprogress_name="http_requests_inprogress",
        inprogress_labels=True,
    )
