# `pipenv`

This project uses [`pipenv`](https://pipenv.pypa.io/en/latest/), which is a relatively new to KBase projects. Traditionally KBase Python projects have used virtual environments, `pip`, and a `requirements.txt` file listing the project dependencies.

`pipenv` does not work too differently, but has different installation instructions. Internally, `pipenv` does create and use a virtual environment and uses `pip` for dependency management.

`pipenv` stores dependency information in a configuration file named `Pipfile`. It creates a companion file `Pipfile.lock` containing the resolved dependencies. The lock file will be used for installation, unless the `Pipfile` has been updated, in which case it will be updated first.

I'm sure we'll refine our usage of `pipenv` in the near future.

## Setting up `pipenv` on your host machine

Note that when using a docker workflow, there is less (and hopefully, ultimately no) need for running `pipenv` or Python on your host machine - it can all be run through a container via a well-formed Dockerfile.

## Using `pipenv` on macOs

This section describes how to get `pipenv` up and running on macOS. These instructions were developed and tested on macOS Monterey.

- install python and pip:

  ```sh
  sudo port install python37
  sudo port select --set python python37
  sudo port select --set python3 python37

  sudo port install py37-pip
  sudo port select --set pip pip37
  sudo port select --set pip3 pip37
  ```

  This need only done once, to set up the mac

- install `pipenv`:

  ```sh
  pip install --user -r requirements.txt
  ```

  `pipenv` will be installed by `pip` into `~/Library/Python/3.7/lib/python/site-packages` (module) and `~/Library/Python/3.7/bin` (executable).

  It will be available to your host user account only.

- ensure `pipenv` is runnable:

  If you look for `pipenv` from the command line, you'll find it is not found:

  ```sh
  % which pipenv
  pipenv not found
  ```

  This is because `~/Library/Python/3.7/bin` is not in the path.

  `pipenv` is perfectly usable, however, as a module via `python`:

  ```sh
  python -m pipenv
  ```

  However, for command line usage, you need to edit `~/.zprofile`, the profile for zsh, to ensure it is in your executable path:

  ```sh
  export PATH="${HOME}/Library/Python/3.7/bin:${PATH}"
  ```

  where the first path element is for Python installed binaries.

  Another option is to install `pipenv` via macports. This would be friendlier to macports, but will install yet another version of Python (3.9 at time of writing) and is not compatible with the way `pipenv` is installed for the GHA workflow.

  Typically Python binaries are installed in a virtual environment, which sets up paths itself.

- Install runtime and dev dependencies

    ```sh
    pipenv install --dev
    ```

- Enter `pipenv` shell in order to activate that virtual environment:

  ```sh
  pipenv shell
  ```

  You can now use Python in this virtual environment, picking up all installed dependencies.

### Removing `pipenv`

`pipenv` will automatically create a virtual environment for you, stored in a location like:

```sh
/Users/YOURACCOUNT/.local/share/virtualenvs/temp-Y3u9pGcy
```

`pipenv` creates this virtual env based on your current directory, and knows how to locate it when you issue `pipenv` commands in that directory.

When you are done with this virtual environment, it will remain in that location until you remove it. This differs from a typical Python virtual environment which resides solely in the directory in which it is created, and will be removed when that directory is removed.

To remove the virtual environment run

```sh
pipenv --rm
```

within the directory in which you ran `pipenv` previously.

## Using `pipenv` on Linux

> TODO: Please contribute to this section if you develop on Linux

## Using `pipenv` on Windows

> TODO: Please contribute this section if you develop on  Windows

## IDEs

Development IDEs may provide direct pipenv support, which is critical for code analysis tools which, for example, highlight unknown imports or typing issues. With pipenv support, the IDE can index all dependencies, and even provide the ability to manage dependencies via their interfaces. 

### Visual Studio Code

- First, in the `pipenv` shell, enter `which python`.

    E.g. 

    ```sh
    ((sample_service) ) YOURACCOUNT@HOSTNAME sample_service % which python
    /Users/YOURACCOUNT/.local/share/virtualenvs/sample_service-reumRtLV/bin/python
    ((sample_service) ) YOURACCOUNT@HOSTNAME sample_service % 
    ```

- Copy the resulting file path.

    E.g.

    ```sh
    /Users/YOURACCOUNT/.local/share/virtualenvs/sample_service-reumRtLV/bin/python
    ```

- Then we set this as the interpreter:

  - First press `[Shift][Command]P` to bring up the command palette
    - Alternatively you may click the "Select Python Interpreter" in the status bar at the bottom of the VSC window

  - Enter `Python: Select Interpreter`, or enough of that for this item to appear in the command palette

  - Select `+ Enter interpreter path...`

  - Paste in the copied path and hit Return

- You should have a fully functioning Python development setup for VSC!

### Your Favorite IDE

> TODO: Please contribute a section for your favorite IDE!