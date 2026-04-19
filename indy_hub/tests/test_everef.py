from indy_hub.services.everef import summarize_job_fees


def test_summarize_job_fees_includes_all_sections_by_default():
    payload = {
        "manufacturing": {
            "job": {"total_job_cost": 100, "total_cost": 130},
        },
        "invention": {
            "job": {"total_job_cost": 40, "total_cost": 55},
        },
    }

    summary = summarize_job_fees(payload)

    assert summary["total_job_cost"] == 140.0
    assert summary["total_api_cost"] == 185.0
    assert summary["section_job_costs"] == {
        "manufacturing": 100.0,
        "invention": 40.0,
    }


def test_summarize_job_fees_can_limit_to_manufacturing_section():
    payload = {
        "manufacturing": {
            "job": {"total_job_cost": 100, "total_cost": 130},
        },
        "copying": {
            "job": {"total_job_cost": 25, "total_cost": 35},
        },
        "reaction": {
            "job": {"total_job_cost": 60, "total_cost": 75},
        },
    }

    summary = summarize_job_fees(payload, included_sections={"manufacturing"})

    assert summary["total_job_cost"] == 100.0
    assert summary["total_api_cost"] == 130.0
    assert summary["section_job_costs"] == {"manufacturing": 100.0}
