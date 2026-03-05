"""Load project configurations from .env files or from the command line.

Provides easy access to paths and credentials used in the project.
Meant to be used as an imported module.

If `settings.py` is run on its own, it will create the appropriate
directories.

For information about the rationale behind decouple and this module,
see https://pypi.org/project/python-decouple/

Note that decouple mentions that it will help to ensure that
the project has "only one configuration module to rule all your instances."
This is achieved by putting all the configuration into the `.env` file.
You can have different sets of variables for difference instances,
such as `.env.development` or `.env.production`. You would only
need to copy over the settings from one into `.env` to switch
over to the other configuration, for example.


Example
-------
Create a file called `myexample.py` with the following content:
```
from settings import config
DATA_DIR = config("DATA_DIR")

print(f"Using DATA_DIR: {DATA_DIR}")
```
and run
```
>>> python myexample.py --DATA_DIR=/path/to/data
/path/to/data
```
and compare to
```
>>> export DATA_DIR=/path/to/other
>>> python myexample.py
/path/to/other
```

"""

import sys
from datetime import datetime
from pathlib import Path
from platform import system

from decouple import config as _config


def find_all_caps_cli_vars(argv=sys.argv):
    """Find all command line arguments that are all caps.

    Find all command line arguments that are all caps and defined
    with a long option, for example, --DATA_DIR or --MANUAL_DATA_DIR.
    When that option is found, the value of the option is returned.

    For example, if the command line is:
    ```
    python settings.py --DATA_DIR=/path/to/data --MANUAL_DATA_DIR=/path/to/manual_data
    ```
    Then the function will return:
    ```
    {'DATA_DIR': '/path/to/data', 'MANUAL_DATA_DIR': '/path/to/manual_data'}
    ```

    For example:
    ```
    >>> argv = [
        '/opt/homebrew/Caskroom/mambaforge/base/envs/ftsf/lib/python3.12/site-packages/ipykernel_launcher.py',
        '--f=/Users/jbejarano/Library/Jupyter/runtime/kernel-v37ea18e94713e364855d5610175b766ee99909eab.json',
        '--DATA_DIR=/path/to/data',
        '--MANUAL_DATA_DIR=/path/to/manual_data'
    ]
    >>> cli_vars = find_all_caps_cli_vars(argv)
    >>> cli_vars
    {'DATA_DIR': '/path/to/data', 'MANUAL_DATA_DIR': '/path/to/manual_data'}
    ```
    """
    result = {}
    i = 0
    while i < len(argv):
        arg = argv[i]
        # Handle --VAR=value format
        if arg.startswith("--") and "=" in arg and arg[2:].split("=")[0].isupper():
            var_name, value = arg[2:].split("=", 1)
            result[var_name] = value
        # Handle --VAR value format (where value is the next argument)
        elif arg.startswith("--") and arg[2:].isupper() and i + 1 < len(argv):
            var_name = arg[2:]
            value = argv[i + 1]
            # Only use this value if it doesn't look like another option
            if not value.startswith("--"):
                result[var_name] = value
                i += 1  # Skip the next argument since we used it as a value
        i += 1
    return result


cli_vars = find_all_caps_cli_vars()

########################################################
## Define defaults
########################################################
defaults = {}

# Absolute path to root directory of the project
if "BASE_DIR" in cli_vars:
    defaults["BASE_DIR"] = Path(cli_vars["BASE_DIR"])
else:
    defaults["BASE_DIR"] = Path(__file__).absolute().parent.parent


# OS type
def get_os():
    os_name = system()
    if os_name == "Windows":
        return "windows"
    elif os_name == "Darwin":
        return "nix"
    elif os_name == "Linux":
        return "nix"
    else:
        return "unknown"


if "OS_TYPE" in cli_vars:
    defaults["OS_TYPE"] = cli_vars["OS_TYPE"]
else:
    defaults["OS_TYPE"] = get_os()


## Dates
defaults["START_DATE"] = datetime.strptime("2020-01-01", "%Y-%m-%d")
defaults["END_DATE"] = datetime.strptime("2025-12-31", "%Y-%m-%d")


## File paths
def if_relative_make_abs(path):
    """If a relative path is given, make it absolute, assuming
    that it is relative to the project root directory (BASE_DIR)

    Example
    -------
    ```
    >>> if_relative_make_abs(Path('_data'))
    WindowsPath('C:/Users/jdoe/GitRepositories/cookiecutter_chartbook/_data')

    >>> if_relative_make_abs(Path("C:/Users/jdoe/GitRepositories/cookiecutter_chartbook/_output"))
    WindowsPath('C:/Users/jdoe/GitRepositories/cookiecutter_chartbook/_output')
    ```
    """
    path = Path(path)
    if path.is_absolute():
        abs_path = path.resolve()
    else:
        abs_path = (defaults["BASE_DIR"] / path).resolve()
    return abs_path


defaults = {
    "DATA_DIR": if_relative_make_abs(Path("_data")),
    "MANUAL_DATA_DIR": if_relative_make_abs(Path("data_manual")),
    "OUTPUT_DIR": if_relative_make_abs(Path("_output")),
    **defaults,
}

# Derived paths (subdirs of DATA_DIR / OUTPUT_DIR)
defaults["PROCESSED_DIR"] = defaults["DATA_DIR"] / "processed_data"
defaults["RESULTS_DIR"] = defaults["OUTPUT_DIR"] / "results"
defaults["IMAGES_DIR"] = defaults["OUTPUT_DIR"] / "images"

# Pipeline: forecast periods and data prep
defaults["DATA_START_DATE"] = "1985-01-01"  # WRDS / rolling window start
defaults["FORECAST_PERIODS"] = ["Q1", "Q2", "Q3", "A1", "A2"]
defaults["COLS_TO_DROP_PREP"] = [
    "adjust_factor", "ticker", "cusip", "cname", "fpedats", "statpers",
    "announcement_actual_eps", "announcement_past_ep", "public_date", "fpi",
]
defaults["TRIM_VALUE"] = 10
defaults["VARS_TO_TRIM"] = ["adj_actual", "meanest", "adj_past_eps"]
defaults["ROLLING_START_YEAR"] = 1985
defaults["ROLLING_END_YEAR"] = 2019
defaults["ROLLING_TRAIN_LENGTH"] = 11
defaults["ROLLING_N_LOOPS"] = 408
defaults["ROLLING_TRAIN_LENGTH_A2"] = 23
defaults["ROLLING_N_LOOPS_A2"] = 396

# Random Forest (train_rf / partial_dependence / functions)
defaults["RF_N_ESTIMATORS"] = 2000
defaults["RF_MAX_DEPTH"] = 7
defaults["RF_MAX_SAMPLES"] = 0.01
defaults["RF_MIN_SAMPLES_LEAF"] = 5
defaults["RF_N_JOBS"] = -1

# Stat analysis
defaults["POST_REGULATION_DATE"] = "2000-10"

# Partial dependence plot
defaults["PDP_DEFAULT_PERIOD"] = "Q1"
defaults["PDP_GRID_RESOLUTION"] = 100
defaults["WINSORIZE_LIMITS"] = (0.01, 0.01)

# Bias analysis / figures
defaults["BIAS_PLOT_YEAR_LOCATOR"] = 2
defaults["BIAS_FIGSIZE"] = (15, 10)
defaults["BIAS_DPI"] = 80
defaults["OUTPUT_DPI"] = 100


def config(
    var_name,
    default=None,
    cast=None,
    settings_py_defaults=defaults,
    cli_vars=cli_vars,
    convert_dir_vars_to_abs_path=True,
):
    """Config defines a variable that can be used in the project. The definition of variables follows
    an order of precedence:
    1. Command line arguments
    2. Environment variables
    3. Settings.py file
    4. Defaults defined in-line in the local file
    5. Error
    """

    # 1. Command line arguments (highest priority)
    if var_name in cli_vars and cli_vars[var_name] is not None:
        value = cli_vars[var_name]
        # Apply cast if provided
        if cast is not None:
            value = cast(value)
        if "DIR" in var_name and convert_dir_vars_to_abs_path:
            value = if_relative_make_abs(Path(value))
        return value

    # 2. Environment variables through decouple
    # Use decouple but with a sentinel default to detect if it was found
    env_sentinel = object()
    env_value = _config(var_name, default=env_sentinel)
    if env_value is not env_sentinel:
        # Found in environment
        if cast is not None:
            env_value = cast(env_value)
        if "DIR" in var_name and convert_dir_vars_to_abs_path:
            env_value = if_relative_make_abs(Path(env_value))
        return env_value

    # 3. Settings.py defaults dictionary
    if var_name in defaults:
        default_value = defaults[var_name]
        # If default_value is directly usable (not a dict with metadata)
        if cast is not None:
            default_value = cast(default_value)
        return default_value

    # 4. Use the default value provided in the local file. Error if not found
    try:
        return _config(var_name, default=default, cast=cast)
    except Exception as e:
        raise ValueError(
            f"Configuration variable '{var_name}' is not defined. "
            f"Please set it via:\n"
            f"  1. Command line: --{var_name}=value\n"
            f"  2. Environment variable: export {var_name}=value\n"
            f"  3. .env file: {var_name}=value\n"
            f"Original error: {e}"
        ) from e


def create_directories():
    config("DATA_DIR").mkdir(parents=True, exist_ok=True)
    config("OUTPUT_DIR").mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    create_directories()
