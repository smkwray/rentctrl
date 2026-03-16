from __future__ import annotations

ACS_PROFILE_STATIC_VARIABLES = {
    "median_household_income": "DP03_0062E",
    "renter_households": "DP04_0047E",
    "renter_share_pct": "DP04_0047PE",
}


def get_acs_profile_variables_for_year(year: int) -> list[str]:
    variables = list(ACS_PROFILE_STATIC_VARIABLES.values())
    if year <= 2014:
        variables.extend(["DP04_0132E", "DP04_0139PE", "DP04_0140PE"])
    else:
        variables.extend(["DP04_0134E", "DP04_0141PE", "DP04_0142PE"])
    if year <= 2018:
        variables.extend(["DP02_0079PE", "DP02_0080PE", "DP02_0084PE"])
    else:
        variables.extend(["DP02_0080PE", "DP02_0081PE", "DP02_0085PE"])
    return variables

STATE_NAME_TO_ABBR = {
    "Arizona": "AZ",
    "California": "CA",
    "Colorado": "CO",
    "Florida": "FL",
    "Georgia": "GA",
    "Idaho": "ID",
    "Nevada": "NV",
    "North Carolina": "NC",
    "Oregon": "OR",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Virginia": "VA",
    "Washington": "WA",
}

STATE_ABBR_TO_FIPS = {
    "AZ": "04",
    "CA": "06",
    "CO": "08",
    "FL": "12",
    "GA": "13",
    "ID": "16",
    "NV": "32",
    "NC": "37",
    "OR": "41",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VA": "51",
    "WA": "53",
}

CORE_TREATED_STATES = ["California", "Oregon"]
DESCRIPTIVE_EXTENSION_STATES = ["Washington"]
DONOR_POOL_STATES = [
    "Arizona",
    "Colorado",
    "Florida",
    "Georgia",
    "Idaho",
    "Nevada",
    "North Carolina",
    "Tennessee",
    "Texas",
    "Utah",
    "Virginia",
]
ALL_SELECTED_STATES = CORE_TREATED_STATES + DESCRIPTIVE_EXTENSION_STATES + DONOR_POOL_STATES
