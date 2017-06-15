
# Contribution Guidelines for DIRAC


## Pull Requests

When making a `Pull Request` please explain what and why things were
changed. Please include a short description in the
BEGINRELEASENOTES/ENDRELEASENOTES block, which will appear in the
relase.notes, once a new tag is made.

Please prepend the title of your pull request with `[targetReleaseBranch]`.

We are happy if you create pull-requests also if you feature is not ready, yet.
Please mark them as such by adding `[WIP]` to the start of the title. The purpose
of this is, for example, that you want to let other people know you are working
on a given issue. For these work-in-progress pull-request, we propose to have a
check list of things that still need to be done.

## Issue tracking

Use the GitHub issue tracker. Reference the issues that you are working on.
If you notice an issue, consider first creating an issue and then refering to it
in your pull-request and commit messages with `#[issue-id]`.

## Coding Conventions
 * You should follow the [DIRAC Coding Conventions](https://dirac.readthedocs.io/en/latest/DeveloperGuide/CodingConvention/index.html)
 * Your code should not introduce any new pylint warnings, and fix as many existing warnings as possible

## Git workflow

For an explanation of the git(hub) workflow please see
[here](https://github.com/andresailer/tutorial#working-updating-pushing). The DIRAC Development Model is in detail describe in the [documentation](https://dirac.readthedocs.io/en/latest/DeveloperGuide/DevelopmentModel/index.html).
