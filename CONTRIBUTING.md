# Contributing Guidelines

Thanks for considering contributing to **dbt-snowpipe-utils** 🎉

## How to Contribute

1. **Fork & Branch**
   - Fork this repo
   - Create a feature branch: `git checkout -b feature/my-change`

2. **Set Up Dev Environment**
   - Install [dbt-core](https://docs.getdbt.com/docs/core) and the [dbt-snowflake adapter](https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile).
   - Run `dbt deps` to install local dependencies.

3. **Testing Your Changes**
   - Use the included seed `reference__snowpipe_config.csv.example` for examples.
   - Run `dbt seed` then:
     ```bash
     dbt run-operation create_snowpipes --args '{"run_queries": false}'
     ```
     to check SQL generation.

   - Optionally, run with `--args '{"run_queries": true}'` in a dev Snowflake account.

4. **Coding Standards**
   - Add or update docblocks in macros.
   - Ensure Jinja is formatted and variables quoted properly.
   - Prefer safe DDL (`CREATE … IF NOT EXISTS`).

5. **Submitting PRs**
   - Push your branch and open a PR.
   - Fill out the PR template (what changed, why, tests).
   - One feature/fix per PR is preferred.

## Reporting Issues
- Open a [GitHub Issue](../../issues) with details:
  - Steps to reproduce
  - Expected vs actual behavior
  - dbt version, Snowflake version, package version

## Code of Conduct
By contributing, you agree to follow the [Contributor Covenant](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).
