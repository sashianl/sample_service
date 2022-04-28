# testing on a fork

This document describes some techniques for verifying that GHA workflows work as designed. The core of this technique is to use a fork of kbase/sample_service to perform pushes to branches, develop, master, pull requests, and releases.

If there are any mistakes in these steps (or GH makes changes to the interface), or you simply want to improve formatting or add images, please feel free.

## Set up fork

1. Fork repo https://github.com/kbase/sample_service.
2. Create a Personal Access Token (PAT)
   1. Under your user menu (upper right corner of the page)
      1. Settings
      2. Developer settings
      3. Personal access tokens
      4. Generate new token
   2. Under "Note" give it a description
   3. For expiration, you can leave as default 30 days
   4. For scopes, select "write:packages", which will also select other scopes by default
   5. Generate token
   6. copy token
3. Add as secret:
   1. You may find it reassuring to open a new browser window to the repo, just so you don't lose the copied token
   2. Under the repo Settings
      1. Secrets
      2. Actions
      3. New repository secret
      4. Name: GHCR_TOKEN
      5. Value: paste the token copied above
      6. New repository secret
      7. Name: GHCR_USERNAME
      8. Value: paste your username (the account that owns the fork)


## Pull Request (to develop branch)

For a PR to develop, we expect tests to run, without building an image.

1. Create a branch locally and then push it up to your fork.

3. switch to the branch in the GitHub ui for your fork

4. Click the **Contribute** dropdown, then click the **Open Pull Request** button.
    
    ![disabled button](./images/enabled-button.png)

   1. If the branch also exists in the origin repo, you may need to push a small commit to the branch in the fork; this will jiggle the GitHub web ui to allow you to create a Pull Request.
   
   ![disabled button](./images/disabled-button.png)

5. The **base repository** will default to the upstream kbase repo

    ![base repository kbase](./images/base-repository-kbase.png)

6. Switch the base repository to the fork

    ![base repository fork](./images/base-repository-fork.png)

7. Finally, switch the branch to develop

    ![base repository develop](./images/base-repository-develop.png)

8. Create the test PR by clicking the **Create pull request** button

9. Visit the Actions tab

10. You should find that there is one action running, which will be labeled with the PR title

11. If you click on the workflow, you'll be taken to the workflow details page. There you should see that there is one job running, labeled "test/run-tests":

    ![pr workflow running job](./images/job-running.png)

12. Note that only the testing workflow is running.

13. When the workflow is finished successfully, the job status indicator will turn green:

    ![pr workflow completed job](./images/job-finished.png)


## Merge PR (to develop branch)

After merging a PR against develop, we expect tests to run and for an image to be built and pushed. 

1. From the forked repo home page, select the **Pull requests** tab

2. Click the title of the PR

3. Click the **Merge pull request** button

4. Click the **Confirm merge** button

5. Click the **Actions** tab

6. You should see the workflow running, with the branch "develop" showing.

7. It takes about 7 minutes to run the tests and the build

8. When the action is finished, you should find the image stored in GHCR:
   1. Go to the fork repo home page
   2. On the right-hand side click "Packages"
   3. Under the Visibility: dropdown, select Private
      1. Since this is a fork, the packages are considered "private"
      2. You should see "sample_service" listed under the packages
   4. Under "recent tagged image versions" you should see the most recent image is tagged `develop`

## Merging PR (to master) Develop to Master

1. From the forked repo home page, select the "develop" branch

2. You can either use the **Compare & pull request** button or the **Contribute** dropdown, and then the **Open pull request** button.

3. under "base repository" select your fork (it defaults to the upstream repo)

4. under "base:" select the "master" branch

5. Click the "Create pull request" button

6. Examine the Actions tab

7. As in the PR to develop example above, inspect the Actions tab to verify that the workflow is running, and afterwards the "Packages" page to ensure that the image ws pushed to GHCR. You should see an image tagged as a pull request.

8. Next merge this PR to master to trigger the "push to master" workflow.

9. It takes about 7 minutes to run the tests and the build, which you can monitor in the Actions tab; the workflow run will be labeled `master`.

10. When the action is finished, you should find the image stored in GHCR:
    1. Go to the fork repo home page
    2. On the right hand side click "Packages"
    3. Under the Visibility: dropdown, select Private
       1. Since this is a fork, the packages are considered "private"
       2. You should see "sample_service" listed under the packages
    4. Under "recent tagged image versions" you should see the most recent image is tagged `master`

## Exercise a Release

1. From the forked repo home page, select the "develop" branch
2. On the right hand side of the page click "Releases"
3. From the "Releases" page click the button "Create a new release"
4. Click the dropdown "Choose a tag"
5. In "Find or create a new tag" enter "v1.2.3" or some other example release tag, and press the Enter/Return key
6. In "Release title" enter "Release version 1.2.3"; content is arbitrary.
7. Click the green "Publish release" button
8. Check the Actions tab, there should be a workflow run labeled "Release version 1.2.3"
9. Again, wait for the workflow run to finish
10. When the action is finished, you should find the image stored in GHCR:
    1. Go to the fork repo home page
    2. On the right hand side click "Packages"
    3. Under the Visibility: dropdown, select Private
       1. Since this is a fork, the packages are considered "private"
       2. You should see "sample_service" listed under the packages
    4. Under "recent tagged image versions" you should see the most recent image is tagged `v1.2.3`

## Test manual workflow run

1. In your local repo, create a branch and push it to the fork
   1. The branch can be named anything, in this example it is "feature-SAM-238"
   
2. Visit the fork at github

3. Visit the Actions tab

4. Select the workflow named "Manual - Test, Build & Push Image"

5. To the right you should see a blue box with a "Run workflow" dropdown:
    ![Workflow Dispatch panel](./images/workflow-dispatch.png)

6. In the **Branch:** dropdown, select the branch you created and pushed:
    ![Branch dropdown](./images/branch-dropdown.png)

7. Click the **Run workflow** button

8. You should find that there is one action running, whose name is the branch name

   
9. When the action is finished, you should find the image stored in GHCR:
   1. Go to the fork repo home page
   2. On the right hand side click "Packages"
   3. Under the Visibility: dropdown, select Private
      1. Since this is a fork, the packages are considered "private"
      2. You should see "sample_service" listed under the packages
   4. Under "recent tagged image versions" you should see a tag that is the same as the branch name


