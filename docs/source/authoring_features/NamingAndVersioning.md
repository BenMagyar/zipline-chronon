---
title: "Naming and Versioning"
order: 8
---


# Naming and Versioning

## Naming

`GroupBy`s, `Staging Queries`s and `Join`s are all identified by three components:

1. **Team**: Files are organized inside a team directory. I.e. `group_bys/my_team/...`
2. **File path**: There may be subdirectories within the team directory, or files directly. I.e. `group_bys/my_team/user_features.py`
3. **Variable name**: Within the file, the entity is assigned to a variable. i.e. `v0 = GroupBy(...)`. See more details below.
4. **Version**: This is an argument to the constructure. I.e. `GroupBy(..., version=0)` See more details below.

These four components combine to fully identify an entity. I.e. `my_team.user_features.v0__0`.

This will be the name that is used for fetching features, and also corresponds to the output table name for backfilled data (`my_team_user_features_v0__0`).


## Variable naming

Note that we often assign a variable name that looks like a version, in the above example `v0`. Since this is simply a python variable name, it can be set to anything (i.e. `base_features_v0`). Using a `v{N}` at the end is a best practice.

**When to use a simple `v{N}` name versus a longer one?**

Generally, if you're only putting a single `GroupBy`, `Staging Queries` or `Join` entity within your file, you can use a simple `v{N}` name.

If, however, you have multiple within the same file, you might want to differentiate them with a more descriptive variable name. This can happen if you're creating two similar GroupBys with a small but meaningful difference in definition. 

For example, say you have features defined on user activities, and you want one set that filter out bot traffic and one that doesn't.

Inside your `user_activities.py` file you might define:

```
no_bots_v0 = GroupBy(...) # Includes a filter clause on the source

with_bots_v0 = GroupBy(...) # No filter clause
```

## Versioning

`GroupBy`s, `Staging Queries`s and `Join`s all take a `version: int` argument in their constructor, i.e.:

```
v0 = GroupBy(
    ...
    version=0
)
```

When compiling, the version becomes part of the name of the entity as a `__{v}` suffix. In this example it would be `{team}.{file}.v0__0`.

### Developer flow for iterating on versions

The version argument is helpful When making a change to an existing entity. For example, say you want to add some features to a `GroupBy`. A user would:

1. Make a git branch on their Chronon directory
2. Bump the version on their GroupBy, and add their new feature definitions
3. Test it using the Zipline/Chronon CLI (see docs on testing)
4. (Optionally) Deploy the branch with the new version for A/B testing (see docs on deploying)
5. Merge the branch to production

Doing this with a new version vs with an entirely new entity has a number of benefits:

1. Compute reuse: When running a backfill with your new `GroupBy` or `Join`, only the new features are computed. The unchanged ones are reused from existing backfills wherever possible.
2. Downstream consumers that reference that entity will automatically migrated to the new version when your merge to main. Schema incompatibilities caused by changing/removing features are caught at compile time.
3. Schedule handoff: after the merge, `schedule-all` schedules the new version and retires the old version's schedules automatically — the superseded version stops running on its own.


## Changing the variable name vs changing the version argument

Changing the variable name essentially creates an entirely new entity within Chronon. This has a number of implications:

1. When computing features for the new entity, it would not reuse unchanged computation as extensively as it would with a bump to the `version` argument
2. Downstream consumers would need to explicitly migrate to the new version (as opposed to a version bump where it happens automatically)

You might want to do this if you intend to keep two versions running in production for an extended period. I.e.

```
# Warning -- to be deprecated
v0 = GroupBy(...)

# Use this version instead
v1 = GroupBy(...)
```

This would allow for both `GroupBy`s to be considered production at the same time. Because these are distinct entities rather than versions of one entity, both are scheduled independently — neither retires the other's schedules.

## Best Practices

File Naming:
1. Describe the features being defined at a high level (i.e. `user_activities.py`, `payments.py`, etc.)
2. Keep file names as short as possible
3. Avoid redundancy with team name or variable name

Variable Naming:
1. If only defining a single entity within a file, use a simple `v{N}` naming 
2. Keep variable names short otherwise, and avoid redundancy with the file name
3. Only change the variable name (i.e. `features_v0` -> `feature_v1`) if you want to treat it as a net new entity (otherwise use a version bump for incremental changes)

Versioning:
1. When creating a new entity, start with `version=0`.
2. When iterating on an entity, create a branch and bump the version
3. Iterations on your branch can keep the same version (no need to bump version in between runs while iterating on a branch)

