# Contributing

**This document is for the open-sourced part of Optimus Cirrus and is not relevant for current employees of Morgan
Stanley working on it internally.**

When contributing to this repository, please first discuss the change you wish to make via issue, email, or any other
method with the owners of this repository before making a change.

Please note we have a code of conduct; please follow it in all your interactions with the project.

## Before your first pull request

If you have not previously contributed to the project, you must first create a *Developer Certificate of Origin* (“DCO”)
and include a reference to this DCO in each of your commits. In addition, if you subsequently wish to contribute code
having a different copyright ownership, then you must create a new DCO for such contribution.

To create a DCO please follow these steps:

1. For code you are contributing, determine who is/are the copyright owner(s). Please note that your employer may own
   the copyright in code you have written even where the code was not created during regular working hours. Copyright
   law is variable from jurisdiction to jurisdiction. Accordingly, consult your employer or a lawyer if you are not
   sure.
2. Fill out the [DCO template](dco/dco_template.md) replacing all `<>` terms as appropriate, and place the completed DCO
   in a file under `.github/dco/<your name>` or if you are not the copyright holder then in a file
   under `.github/dco/<your name>-<copyright holder name(s)>`.
    1. Please note that the name you provide (`<your name>`) must be your real
       (legal) name; we will not accept aliases, pseudonyms or anonymous contributions.
    1. If you’ve determined that the copyright holder of the code that you’ve written is an entity other than yourself (
       e.g., your employer), then include the legal name of the copyright holder(s) (`<name of copyright holder(s)>`).
       You must ensure that you are authorized by the copyright holder(s) to be able to grant the licenses under the DCO
       for the purpose of contributing to the project. Negotiating such authorization and administering the terms is
       entirely between you and the copyright holder(s).
3. Issue a pull request adding the DCO file and adding your name and username to [contributors.md](contributors.md)

## Pull request process

When you create a pull request, follow these steps:

1. Your commit message for the code you are submitting must include a
   `“Covered by <dco>“` line which indicates your acceptance of the DCO terms and conditions.
   `<dco>` here is the file name of the DCO.
2. Your commit must include a change to the `NOTICE.txt` file that contains complete details of any applicable copyright
   notice for your submission and including any applicable third party license(s) or other restrictions associated with
   any part of your contribution, and of all matters required to be disclosed under such third party license(s) (such as
   any applicable copyright, patent, trademark, and attribution notices, and any notices relating to modifications made
   to open source software). Note your contribution must retain all applicable copyright, patent, trademark and
   attribution notices.

## Pull request guidelines

* Since tests are not currently public, please describe the manual testing process performed.
* Ensure any install or build artefacts are removed from the pull request.
* We generally prefer squashed commits, unless multi-commits add clarity or are required for mixed copyright commits.
* You may merge the Pull Request in once the build has passed and you have the sign-off of one other developer who works
  on maintaining the project, or if you do not have permission to do that, you may request the reviewer to merge it for
  you. 
   
   