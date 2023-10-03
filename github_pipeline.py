import dlt

from github import github_reactions, github_repo_events


def load_caddy_events() -> None:
    """Loads airflow events. Shows incremental loading. Forces anonymous access token"""
    pipeline = dlt.pipeline(
        "github_events", destination='duckdb', dataset_name="caddy_events"
    )
    data = github_repo_events("rajaseg", "caddy", access_token="")
    print(pipeline.run(data))
    # does not load same events again
    data = github_repo_events("rajaseg", "caddy", access_token="")
    print(pipeline.run(data))


if __name__ == "__main__":
    # load_duckdb_repo_reactions_issues_only()
    load_caddy_events()
    # load_dlthub_dlt_all_data()
