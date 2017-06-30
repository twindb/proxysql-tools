Development
===========

Relase new version
~~~~~~~~~~~~~~~~~~
1. Checkout to develop branch and get clean code:
::

  > git checkout develop
  > git reset --hard
  > git pull

2. Get current version and choose new one:
::

  less setup.cfg
  [bumpversion]
  current_version = <version>

or:
::

  proxysql-tool --version

3. Start release branch using git-flow:
::

  git flow release start <version>
  bumpversion patch | minor | major
  git flow release finish <version>
  Tag commit message "Release <version>"

4. Push:
::

  git push --all
  git push --tags
