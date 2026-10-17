import os

GHA = os.environ.get("GITHUB_ACTIONS") == "true"


def pytest_runtest_logreport(report):
    """On GitHub Actions, surface each test result as an annotation so it can be read from the API."""
    if not GHA or report.when != "call":
        return
    name = report.nodeid.split("::")[-1]
    if report.failed:
        msg = str(report.longrepr).strip().splitlines()
        tail = " / ".join(l.strip() for l in msg[-12:] if l.strip())[:1500]
        print(f"\n::error title={name}::{tail}", flush=True)
    elif report.passed:
        print(f"\n::notice title={name}::passed in {report.duration:.0f}s", flush=True)
