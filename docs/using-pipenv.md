# Pipenv

## Using pipenv on macOs

- install python and pip

```sh
port install python37
sudo port select --set python python37
sudo port select --set python3 python37

port install py37-pip
sudo port select --set pip pip37
sudo port select --set pip3 pip37
```

`pipenv` will be installed with pip in `~/Library/Python/3.7/lib/python/site-packages`.

- ensure that things are in your path:

on a fresh mac with Big Sur, you need to edit `~/.zprofile`, the profile for zsh.

fix your path:

```sh
export PATH="${HOME}/Library/Python/3.7/bin:${HOME}/.local/bin:$PATH"
```

where the first path element is for Python installed binaries, and the second is for `pipx` installed binaries (and other
tools may use this dir also)

> NOT 100% sure that this is required - try without this and see






- Install dev dependencies

```sh
pipenv install --dev
```

- Install runtime dependencies:

```sh
pipenv install
```

- Enter pipenv shell in order to activate that virtual environment:

```sh
pipenv shell
```


Within the shell, you can run any scripts that were installed by pipenv, and of course the library dependencies will be available as well.

## IDEs

### Visual Studio Code

- First, in the `pipenv` shell, enter `which python`.

    E.g. 

    ```sh
    ((sample_service) ) erikpearson@Eriks-MBP sample_service % which python
    /Users/erikpearson/.local/share/virtualenvs/sample_service-reumRtLV/bin/python
    ((sample_service) ) erikpearson@Eriks-MBP sample_service % 
    ```

- Copy the resulting file path.

    E.g.

    ```sh
    /Users/erikpearson/.local/share/virtualenvs/sample_service-reumRtLV/bin/python
    ```

- Then we set this as the interpreter:

  - First press `[Shift][Command]P` to bring up the command palette
    - Alternatively you may click the "Select Python Interpreter" in the status bar at the bottom of the VSC window

  - Enter `Python: Select Interpreter`, or enough of that for this item to appear in the command palette

  - Select `+ Enter interpreter path...`

  - Paste in the copied path and hit Return


