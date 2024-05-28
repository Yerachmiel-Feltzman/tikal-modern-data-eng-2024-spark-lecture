# Local development:

## First time setup

### Python 3

Suggested installation:

- [pyenv](https://github.com/pyenv/pyenv) is an easy way to install python
- it'll automatically use the [.python-version](.python-version) file in the project's root folder
- after installing `pyenv` correctly, simply run `pyenv install`

### Java 8 (will probably also work with 11, 17, 21, or 23)

Suggested installation:

- [SDKMAN](https://sdkman.io/install) is an easy way to install Java
- it'll automatically use the [.sdkmanrc](.sdkmanrc) file in the project's root folder
- if you use `sdkman`, simply run `sdk env install`

Make sure it's all set by running `echo $JAVA_HOME`, `java -version` and `javac -version`

### Poetry

Install Poetry: https://python-poetry.org/docs/#installation

Install dependencies:

```
poetry install --no-root
```

### Test all is set up:

Spawn and venv shell:

```commandline
poetry shell
```

Test it works:

```commandline
 pytest tests/test_setup.py -s  
```

It should return a dataframe like this:

```
+---+---+-------+----+----+                                                     
| _1| _2|     _3|  _4|  _5|
+---+---+-------+----+----+
|  I| am|running|just|fine|
+---+---+-------+----+----+
```

## Set up your IDE (optional):

Run:

```commandline
poetry env info
```

It will return something like this:

```
Virtualenv
Python:         3.11.4
Implementation: CPython
Path:           /Users/yf/Library/Caches/pypoetry/virtualenvs/tikal-modern-data-eng-2024-spark-lecture-b5bscjAp-py3.11
Executable:     /Users/yf/Library/Caches/pypoetry/virtualenvs/tikal-modern-data-eng-2024-spark-lecture-b5bscjAp-py3.11/bin/python
Valid:          True

System
Platform:   darwin
OS:         posix
Python:     3.11.4
Path:       /Users/yf/.pyenv/versions/3.11.4
Executable: /Users/yf/.pyenv/versions/3.11.4/bin/python3.11
```

Take the `Virtualenv.Executable` path and make it the project interpreter in your IDE.







