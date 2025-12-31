"""
Testing  Retention Curves
--------------------------
Future Improvements:
- Add more comprehensive tests.
"""

from main import retention_curves

checking_map = {
    "package_type": "DIGITAL Subscriber",
    "trial_duration_months": 6,
    "term_cadence": "year",
}
retention_curves[
    (retention_curves["package_type"] == checking_map["package_type"])
    & (
        retention_curves["trial_duration_months"]
        == checking_map["trial_duration_months"]
    )
    & (retention_curves["term_cadence"] == checking_map["term_cadence"])
]
