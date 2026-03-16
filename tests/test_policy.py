from pathlib import Path
import pandas as pd

from rent_control_public.policy import aggregate_annual_policy_panel, expand_quarterly_policy_panel, load_policy_events


def test_policy_panel_build():
    state_meta = pd.read_csv(Path("config/state_metadata.csv"), dtype={"state_fips": str})
    events = load_policy_events(Path("config/policy_events_core.csv"))
    q = expand_quarterly_policy_panel(state_meta, events, start="2018Q1", end="2020Q4")
    assert not q.empty
    ca = q[(q["state_abbr"] == "CA") & (q["calendar_period"] == "2020Q1")]
    assert int(ca["policy_active_preferred"].iloc[0]) == 1
    or_q1 = q[(q["state_abbr"] == "OR") & (q["calendar_period"] == "2019Q1")]
    or_q2 = q[(q["state_abbr"] == "OR") & (q["calendar_period"] == "2019Q2")]
    assert int(or_q1["policy_active_preferred"].iloc[0]) == 0
    assert int(or_q2["policy_active_preferred"].iloc[0]) == 1
    annual = aggregate_annual_policy_panel(q)
    assert not annual.empty
