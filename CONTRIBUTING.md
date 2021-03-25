# Contributing guide

Welcome to Percona Backup for MongoDB!

1. [Prerequisites](#prerequisites)
2. [Submitting a pull request](#submitting-a-pull-request)
3. [Contributing to documentation](#contributing-to-documentation)

We're glad that you would like to become a Percona community member and participate in keeping open source open.  

Percona Backup for MongoDB (PBM) is a distributed, low-impact solution for achieving consistent backups of MongoDB sharded clusters and replica sets.

You can contribute in one of the following ways:

1. Reach us on our [Forums](https://forums.percona.com) and [Discord]([https://discord.gg/mQEyGPkNbR](https://discord.gg/mQEyGPkNbR)).
2. [Submit a bug report or a feature request](https://github.com/percona/percona-backup-mongodb/blob/main/README.md) 
3. Submit a pull request (PR) with the code patch
4. Contribute to documentation 

## Prerequisites

Before submitting code contributions, we ask you to complete the following prerequisites.

### 1. Sign the CLA

Before you can contribute, we kindly ask you to sign our [Contributor License Agreement](https://cla-assistant.percona.com/&lt;linktoCLA>) (CLA). You can do this in on click using your GitHub account.

**Note**:  You can sign it later, when submitting your first pull request. The CLA assistant validates the PR and asks you to sign the CLA to proceed.

### 2. Code of Conduct

Please make sure to read and agree to our [Code of Conduct](https://github.com/percona/community/blob/main/content/contribute/coc.md).

## Submitting a pull request

All bug reports, enhancements and feature requests are tracked in [Jira issue tracker](https://jira.percona.com/projects/PBM). Though not mandatory, we encourage you to first check for a bug report among Jira issues and in the PR list: perhaps the bug has already been addressed. 

For feature requests and enhancements, we do ask you to create a Jira issue, describe your idea and discuss the design with us. This way we align your ideas with our vision for the product development.

If the bug hasn’t been reported / addressed, or we’ve agreed on the enhancement implementation with you, do the following:

1. [Fork](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo) this repository
2. Clone this repository on your machine. 
3. Create a separate branch for your changes. If you work on a Jira issue, please  include the issue number in the branch name so it reads as ``<JIRAISSUE>-my_branch``. This makes it easier to track your contribution.
4. Make your changes. Please follow the guidelines outlined in the [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments) to improve code readability.
5. Test your changes locally. See the [Running tests locally](#running-tests-locally) section for more information
6. Commit the changes. Add the Jira issue number at the beginning of your  message subject so that is reads as `<JIRAISSUE> -  My subject`. The [commit message guidelines](https://gist.github.com/robertpainsi/b632364184e70900af4ab688decf6f53) will help you with writing great commit messages
7. Open a PR to Percona
8. Our team will review your code and if everything is correct, will merge it. 
Otherwise, we will contact you for additional information or with the request to make changes.

### Building Percona Backup for MongoDB

To build Percona Backup for MongoDB from source code, you require the following:

* Go 1.11 or above. See [Installing and setting up Go tools](
https://golang.org/doc/install) for more information
* make
* ``krb5-devel`` for Red Hat Enterprise Linux / CentOS or ``libkrb5-dev`` for Debian / Ubuntu. This package is required for Kerberos authentication in Percona Server for MongoDB.

To build the project, run the following commands:

```sh
$ git clone https://github.com/<your_name>/percona-backup-mongodb
$ cd percona-backup-mongodb
$ make build
```

After ``make`` completes, you can find ``pbm`` and ``pbm-agent`` binaries in the ``./bin`` directory:

```sh
$ cd bin
$ ./pbm version
```

By running ``pbm version``, you can verify if Percona Backup for MongoDB has been built correctly and is ready for use.

```
Output

Version:   [pbm version number]
Platform:  linux/amd64
GitCommit: [commit hash]
GitBranch: master
BuildTime: [time when this version was produced in UTC format]
GoVersion: [Go version number]
```

**TIP**: instead of specifying the path to pbm binaries, you can add it to the PATH environment variable:

```sh
export PATH=/percona-backup-mongodb/bin:$PATH
```

### Running tests locally

When you work, you should periodically run tests to check that your changes don’t break existing code.

The tests can be found in the ``e2e-tests`` directory.

To run all tests, use the following command:

```sh
$ MONGODB_VERSION=3.6 ./run-all
```

``$ MONGODB_VERSION`` stands for the Percona Server for MongoDB version PBM is running with. Default is 3.6.

You can run tests on your local machine with whatever operating system you have. After you submit the pull request, we will check your patch on multiple operating systems.

## Contributing to documentation

Percona Backup for MongoDB documentation is written in [reStructured text markup language](https://docutils.sourceforge.io/rst.html) and is created using [Sphinx Python Documentation Generator](https://www.sphinx-doc.org/en/master/). The doc files are in the ``doc/source`` directory.

To contribute to documentation, learn about the following:
- [reStructured text](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html) markup language. 
- [Sphinx](https://www.sphinx-doc.org/en/master/usage/quickstart.html) documentation generator. We use it to convert source ``.rst`` files to html and PDF documents.
- [git](https://git-scm.com/)
- [Docker](https://docs.docker.com/get-docker/). It allows you to run Sphinx in a virtual environment instead of installing it and its dependencies on your machine.

### Edit documentation online via GitHub

1. Click the **Edit this page** link on the sidebar. The source ``.rst`` file of the page opens in GitHub editor in your browser. If you haven’t worked with the repository before, GitHub creates a [fork](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo) of it for you.

2. Edit the page. You can check your changes on the **Preview** tab.

   **NOTE**: GitHub’s native markup language is [Markdown](https://daringfireball.net/projects/markdown/) which differs from the reStructured text. Therefore, though GitHub renders titles, headings and lists properly, some rst-specific elements like variables, directives, internal links will not be rendered.

3. Commit your changes.

	 - In the *Commit changes* section, describe your changes.
	 - Select the **Create a new branch for this commit and start a pull request** option
	 - Click **Propose changes**.

4. GitHub creates a branch and a commit for your changes. It loads a new page on which you can open a pull request to Percona. The page shows the base branch - the one you offer your changes for, your commit message and a diff - a visual representation of your changes against the original page.  This allows you to make a last-minute review. When you are ready, click the **Create pull request** button.
5. Someone from our team reviews the pull request and if everything is correct, merges it into the documentation. Then it gets published on the site.

### Edit documentation locally

This option is for users who prefer to work from their computer and / or have the full control over the documentation process.

The steps are the following:

1. Fork this repository
2. Clone the repository on your machine:

```sh
git clone git@github.com:<your_name>/percona-backup-mongodb.git
```

3. Change the directory to ``percona-backup-mongodb`` and add the remote upstream repository:

```sh
git remote add upstream git@github.com:percona/percona-backup-mongodb.git
```

4. Pull the latest changes from upstream

```sh
git fetch upstream
git merge upstream/main
```

5. Create a separate branch for your changes

```sh
git checkout -b <my_branch>
```

6. Make changes
7. Commit your changes. The [commit message guidelines](https://gist.github.com/robertpainsi/b632364184e70900af4ab688decf6f53) will help you with writing great commit messages

8. Open a pull request to Percona

### Building the documentation

To verify how your changes look, generate the static site with the documentation. This process is called *building*. You can do it in these ways:
- [Use Docker](#use-docker)
- [Install Sphinx and build locally](#install-sphinx-and-build-locally)

#### Use Docker

1. [Get Docker](https://docs.docker.com/get-docker/)
2. We use [this Docker image](https://hub.docker.com/r/ddidier/sphinx-doc) to build documentation. Run the following command:

```sh
docker run --rm -i -v `pwd`:/doc -e USER_ID=$UID ddidier/sphinx-doc:0.9.0 make clean html
```
   If Docker can't find the image locally, it first downloads the image, and then runs it to build the documentation.

3. Go to the ``doc/build/html`` directory and open the ``index.html`` file to see the documentation.
4. Your static site will look different from the one on the web site. This is because we use a Percona theme that is rendered when the documentation is published on the web. To view the documentation with Alabaster theme, run the following command:

```sh
docker run --rm -i -v `pwd`:/doc -e USER_ID=$UID ddidier/sphinx-doc:0.9.0 sphinx-build -b html -D 'html_theme=alabaster' source build/html
```
#### Install Sphinx and build locally

1. Install [pip](https://pip.pypa.io/en/stable/installing/)
2. Install [Sphinx](https://www.sphinx-doc.org/en/master/usage/installation.html).
3. While in the root directory of the doc project, run the following command to build the documentation:

```sh
make clean html
```
4. Go to the ``build/html`` directory and open the ``index.html`` file to see the documentation.
5. Your static site will look different from the one on the web site. This is because we use a Percona theme that is rendered when the documentation is published on the web. To view the documentation with Alabaster theme, run the following command:

```sh
sphinx-build -b html -D 'html_theme=alabaster' source build/html
```
6. To create a PDF version of the documentation, run the following command:

```sh
make clean latexpdf
```

The PDF document is in the ``build/latex`` folder.

If you wish to have a live, themed preview of your changes, do the following:

- Install the [sphinx-autobuild](https://pypi.org/project/sphinx-autobuild/) extension
- Run the following command:

```sh
sphinx-autobuild -D html_theme="alabaster" --open-browser source build/html
```

## After your pull request is merged

Once your pull request is merged, you are an official Percona Community Contributor. Welcome to the community!
