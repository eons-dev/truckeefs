# EBBS for GitHub

Have [ebbs](https://github.com/eons-dev/bin_ebbs) manage your git repo!

## Usage

### Automatic

The easiest way to get started with these workflows and ebbs pipelines is to just run `ebbs -b boilerplate` from the root of your repository.

See [the boilerplate docs](https://github.com/eons-dev/build_boilerplate) for more info.

### Manual

#### build/github.json
Make sure you have a valid EBBS github.json in the build folder of your directory. This would be something like:

```json
{
  "build_in" : "github",
  "clear_build_path" : true,
  "next": [
    {
      "build" : "publish",
      "run_when_any" : "release",
      "copy" : [
        {"../../inc/" : "inc/"}
      ],
      "config" : {
        "clear_build_path" : false,
        "visibility" : "public"
      }
    }
  ]
}
```


#### Subrepo

Github Actions cannot use `git submodule`. Thus, we must use [git subrepo](https://github.com/ingydotnet/git-subrepo).

Clone with subrepo:
```shell
mkdir -p .github/workflows
git subrepo clone https://github.com/eons-dev/part_ebbs-workflows.git .github/workflows
```

Alternatively to using `subrepo`, you can manually download and extract the files in this repo to `.github/workflows`.

## Features:

All ebbs workflows call your `build/build.json`.

### Activity Workflows

The following events trigger ebbs workflows:
 * "release"
 * "push"
 * "pull_request"

Any of those events can be used with the `"run_when_..."` json varr, allowing you to use a single build.json for all your workflows. See [the ebbs docs](https://github.com/eons-dev/bin_ebbs) for more info

For example:
```json
{
  "run_when_any": [
    "pull_request"
  ]
}
```

If you would like additional environment variables, cli args, build steps, etc, you can always clone this repo or modify the subrepo clone to your liking.

### Scheduled Workflows

The following scheduled events triggers are also available:
 * `"daily"` (runs at 0000 UTC)
 * `"weekly"` (runs at 0100 UTC)
 * `"monthly"` (runs at 0200 UTC)

Each scheduled trigger is set to run 1 hour after the preceding frequencies trigger so that any conflicting behavior can be executed with an order of known precedence. If your ebbs execution takes longer than 1 hour, let us know and we can multiply the offset.

### Automatic Updates

Coming soon!

