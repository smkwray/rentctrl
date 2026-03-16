from __future__ import annotations

from rent_control_public.east_palo_alto import parse_event_page, parse_search_results, summarize_board_archive


SEARCH_HTML = """
<ol class="search-results node-results">
<li class="search-result">
  <h2 class="title"><a href="https://www.cityofepa.org/rent-stabilization/page/rent-stabilization-board-meeting">Rent Stabilization Board Meeting</a></h2>
  <div class="search-snippet-info"><p class="search-snippet">Rent Stabilization Board Meeting Calendar Date</p></div>
</li>
<li class="search-result">
  <h2 class="title"><a href="https://www.cityofepa.org/rent-stabilization/page/rent-stabilization-program-regular-board-meeting-0">Rent Stabilization Program Regular Board Meeting</a></h2>
  <div class="search-snippet-info"><p class="search-snippet">Program Regular Board Meeting</p></div>
</li>
</ol>
"""


EVENT_HTML = """
<html><head><title>Rent Stabilization Board Meeting - CANCELED | City of East Palo Alto</title></head>
<body>
<span class="date-display-single">Wednesday, February 11, 2026 - 7:00pm</span>
<a href="/calendar/ical/node/25083/calendar.ics">Outlook (iCal)</a>
<a href="https://calendar.google.com/calendar/r/eventedit?dates=20260212T030000Z/20260212T030000Z&text=Rent+Stabilization+Board+Meeting+-+CANCELED">Google</a>
<div class='repeat_rule_expand'><div><ul><li>01-14-2026</li><li>02-11-2026</li></ul></div></div>
<h2><a href="http://eastpaloalto.iqm2.com/Citizens/default.aspx">View Agenda Here</a></h2>
</body></html>
"""


def test_parse_search_results() -> None:
    df = parse_search_results(SEARCH_HTML, page=0)
    assert len(df) == 2
    assert df.iloc[0]["title"] == "Rent Stabilization Board Meeting"
    assert "Calendar Date" in df.iloc[0]["snippet"]


def test_parse_event_page() -> None:
    event = parse_event_page(EVENT_HTML, url="https://www.cityofepa.org/test")
    assert event["is_canceled"] is True
    assert event["event_datetime"] == "Wednesday, February 11, 2026 - 7:00pm"
    assert str(event["ical_url"]).endswith("/calendar/ical/node/25083/calendar.ics")
    assert event["repeat_dates_count"] == 2
    assert event["agenda_url"] == "http://eastpaloalto.iqm2.com/Citizens/default.aspx"


def test_summarize_board_archive() -> None:
    search_df = parse_search_results(SEARCH_HTML, page=0)
    import pandas as pd
    event_df = pd.DataFrame([parse_event_page(EVENT_HTML, url="https://www.cityofepa.org/test")])
    summary = summarize_board_archive(search_df, event_df)
    assert int(summary.loc[summary["metric"] == "search_rows", "value"].iloc[0]) == 2
    assert int(summary.loc[summary["metric"] == "event_pages_with_agenda_url", "value"].iloc[0]) == 1
