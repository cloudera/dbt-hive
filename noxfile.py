import nox

package = "dbt-hive"
python_versions = ["3.7", "3.8", "3.9", "3.10", "3.11"]
dbt_versions = ["1.5.0"]


@nox.session(python=python_versions)
@nox.parametrize("dbt_version", dbt_versions)
def tests(session, dbt_version: str) -> None:
    # Define the profile used by the dbt
    PROFILE = "dwx_endpoint"
    # Install dbt-core
    session.run_always("pip", "install", f"dbt-core=={dbt_version}")
    # Install dev dependencies
    dev_setup(session)

    TESTS = session.posargs[0] if session.posargs else ""
    DEBUG = session.posargs[1] if len(session.posargs) > 1 else "false"
    if DEBUG == "true":
        session.run(
            "python",
            "-m",
            "pytest",
            "--profile",
            PROFILE,
            TESTS,
            "--capture=no",
            env={"DBT_DEBUG": "true"},
        )
    else:
        session.run("python", "-m", "pytest", "-n", "auto", "--profile", PROFILE, TESTS)


@nox.session(name="dev_setup", reuse_venv=True)
def dev_setup(session):
    # Install development dependencies
    session.install("--upgrade", "pip")
    session.install("-e", ".")
    session.install("-r", "dev-requirements.txt")
