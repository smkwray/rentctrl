from __future__ import annotations

from rent_control_public.santa_monica import _extract_hidden_value, parse_mar_results, summarize_mar_pilot


SAMPLE_HTML = """
<html><body>
<input type="hidden" name="__VIEWSTATE" value="abc123" />
<input type="hidden" name="__EVENTVALIDATION" value="def456" />
<table class="margridview" id="ctl00_mainContent_gvMarData">
<tr class="margridviewheader">
<th>Address</th><th>Unit</th><th>MAR</th><th>Tenancy Date</th><th>Bedrooms</th><th>Parcel</th>
</tr>
<tr><td>624 LINCOLN BLVD</td><td>A</td><td align="right">$3,373</td><td>3/1/2021</td><td>2</td><td>4293011005</td></tr>
<tr><td>624 LINCOLN BLVD</td><td>B</td><td align="right">$934</td><td>&nbsp;</td><td>2</td><td>4293011005</td></tr>
</table>
</body></html>
"""


def test_extract_hidden_value() -> None:
    assert _extract_hidden_value(SAMPLE_HTML, "__VIEWSTATE") == "abc123"
    assert _extract_hidden_value(SAMPLE_HTML, "__EVENTVALIDATION") == "def456"


def test_parse_mar_results() -> None:
    df = parse_mar_results(SAMPLE_HTML)
    assert len(df) == 2
    assert list(df.columns) == ["address", "unit", "mar", "tenancy_date", "bedrooms", "parcel"]
    assert df.iloc[0]["address"] == "624 LINCOLN BLVD"
    assert df.iloc[1]["tenancy_date"] == ""


def test_summarize_mar_pilot() -> None:
    df = parse_mar_results(SAMPLE_HTML)
    df["search_street_number"] = "624"
    df["search_street_name"] = "Lincoln Blvd"
    summary = summarize_mar_pilot(df, queries_run=3)
    assert int(summary.loc[summary["metric"] == "rows_returned", "value"].iloc[0]) == 2
    assert int(summary.loc[summary["metric"] == "unique_parcels", "value"].iloc[0]) == 1
    assert int(summary.loc[summary["metric"] == "queries_run", "value"].iloc[0]) == 3
