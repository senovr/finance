import time_convertor


def format_dt_str(dt):

    if dt < 1.0:
        dt_str = "%.3f" % dt
    elif dt < 10.0:
        dt_str = "%.2f" % dt
    elif dt < 100.0:
        dt_str = "%.1f" % dt
    else:
        dt_str = "%.0f" % dt

    return dt_str


def format_timer_string(dt=0.0, percent_complete=None, time_format=""):

    timer_string = "elapsed time " + format_dt_str(dt) + " s;"

    if percent_complete is not None:

        expected_sec_to_complete = dt * 100.0 / max(percent_complete, 0.01)

        if time_format == "":
            str_expected = format_dt_str(expected_sec_to_complete - dt) + "s;"
            str_total = format_dt_str(expected_sec_to_complete) + "s;"
        else:
            str_expected = time_convertor.convert_via_naive(
                expected_sec_to_complete - dt
            )
            str_total = time_convertor.convert_via_naive(expected_sec_to_complete)

        timer_string = (
            timer_string
            + " expected to end ~ "
            + str_expected
            + " total time ~ "
            + str_total
        )

    return timer_string
