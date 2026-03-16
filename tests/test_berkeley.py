from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from rent_control_public.berkeley import (
    BerkeleyGuestCredentials,
    BerkeleyLookupError,
    _extract_visible_text,
    authenticate_public_session,
    build_search_payload,
    discover_lookup_mechanics,
    encrypt_password_with_openssl,
    get_homepage_credentials,
    parse_search_results,
    run_pilot_sample,
    summarize_pilot,
)


class TestTextHelpers:
    def test_extract_visible_text_strips_scripts_and_styles(self):
        html = "<p>Hello</p><script>var x=1;</script><style>.a{}</style><p>World</p>"
        assert _extract_visible_text(html) == ["Hello", "World"]


class TestDiscovery:
    def test_discovery_reports_live_api_shape(self):
        with patch("rent_control_public.berkeley.get_homepage_credentials", return_value=BerkeleyGuestCredentials("BERK", "VAPAPP")):
            info = discover_lookup_mechanics()
        assert info["public_username"] == "BERK"
        assert info["public_password_present"] is True
        assert "Public API confirmed" in info["blocker_summary"]


class TestConfigAndAuth:
    def test_get_homepage_credentials_reads_config(self):
        session = MagicMock()
        session.get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "configParam": [
                    {"paramName": "homePageLoginUserName", "paramValue": "BERK"},
                    {"paramName": "homePageLoginPassword", "paramValue": "VAPAPP"},
                ]
            },
        )
        creds = get_homepage_credentials(session=session)
        assert creds.username == "BERK"
        assert creds.password == "VAPAPP"

    def test_encrypt_password_wraps_openssl_output(self):
        completed = MagicMock(stdout=b"abc123")
        with patch("rent_control_public.berkeley.subprocess.run", return_value=completed):
            encrypted = encrypt_password_with_openssl("secret", "PUBLICKEY")
        assert encrypted == "YWJjMTIz"

    def test_authenticate_public_session_posts_expected_form(self):
        session = MagicMock()
        session.post.return_value = MagicMock(status_code=200, json=lambda: {"access_token": "TOKEN"})
        with patch("rent_control_public.berkeley.get_homepage_credentials", return_value=BerkeleyGuestCredentials("BERK", "VAPAPP")), patch(
            "rent_control_public.berkeley.get_public_key", return_value="PUBLICKEY"
        ), patch("rent_control_public.berkeley.encrypt_password_with_openssl", return_value="ENC"):
            token = authenticate_public_session(session=session)
        assert token == "TOKEN"
        posted_data = session.post.call_args.kwargs["data"]
        assert "username=BERK" in posted_data
        assert "password=ENC" in posted_data
        assert "authType=apiclient" in posted_data


class TestSearchParsing:
    def test_build_search_payload(self):
        payload = build_search_payload("2000 University Ave")
        assert payload["keyword"] == "2000 University Ave"
        assert "ADDRESS" in payload["cityService"]

    def test_parse_search_results_extracts_apn_and_address(self):
        payload = {
            "response": [
                {
                    "title": "057202501301",
                    "type": "ADDRESS",
                    "matchingCount": 0,
                    "parameters": {
                        "additional": {
                            "apnNumber": "057202501301",
                            "totalUnits": 0,
                            "siteAddresses": [
                                {"fullAddress": "2000 UNIVERSITY AVE, BERKELEY, CA 94704"}
                            ],
                        }
                    },
                }
            ]
        }
        df = parse_search_results(payload, keyword="2000 University Ave")
        assert len(df) == 1
        assert df.iloc[0]["apn_number"] == "057202501301"
        assert df.iloc[0]["full_address"] == "2000 UNIVERSITY AVE, BERKELEY, CA 94704"


class TestPilotSample:
    def test_run_pilot_sample_success(self):
        with patch("rent_control_public.berkeley.authenticate_public_session", return_value="TOKEN"), patch(
            "rent_control_public.berkeley.search_content_count",
            return_value={"count": {"ADDRESS": 1, "APN": 0, "CASE": 0}},
        ), patch(
            "rent_control_public.berkeley.search_content",
            return_value={
                "totalRecord": 1,
                "response": [
                    {
                        "title": "057202501301",
                        "type": "ADDRESS",
                        "matchingCount": 0,
                        "parameters": {
                            "additional": {
                                "apnNumber": "057202501301",
                                "totalUnits": 0,
                                "siteAddresses": [{"fullAddress": "2000 UNIVERSITY AVE"}],
                            }
                        },
                    }
                ],
            },
        ):
            detail_df, summary_df = run_pilot_sample([{"address": "2000 University Ave", "unit": ""}])
        assert len(detail_df) == 1
        assert summary_df.iloc[0]["rows_returned"] == 1
        assert summary_df.iloc[0]["address_rows"] == 1

    def test_run_pilot_sample_handles_errors(self):
        with patch(
            "rent_control_public.berkeley.authenticate_public_session",
            side_effect=BerkeleyLookupError("bad auth"),
        ):
            with pytest.raises(BerkeleyLookupError, match="bad auth"):
                run_pilot_sample([{"address": "2000 University Ave", "unit": ""}])


class TestSummary:
    def test_summarize_pilot(self):
        summary_df = pd.DataFrame(
            [
                {
                    "keyword": "2000 University Ave",
                    "rows_returned": 1,
                    "address_rows": 1,
                    "apn_rows": 0,
                    "case_rows": 0,
                    "unique_apn_numbers": 1,
                    "fetch_status": "ok",
                },
                {
                    "keyword": "2100 Milvia St",
                    "rows_returned": 0,
                    "address_rows": 0,
                    "apn_rows": 0,
                    "case_rows": 0,
                    "unique_apn_numbers": 0,
                    "fetch_status": "error: timeout",
                },
            ]
        )
        agg = summarize_pilot(summary_df)
        row_map = dict(zip(agg["metric"], agg["value"]))
        assert row_map["total_keywords"] == 2
        assert row_map["successful_keywords"] == 1
        assert row_map["total_rows_returned"] == 1
