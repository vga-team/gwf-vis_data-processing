# GWF Vis Data Processing

## Before start

### Creating and activate the virtual environment (optional)

A virtual environment provides a isolated environment for Python packages, which you can install pacakges only for this specific project instead of install it globally.
For Linux system, sample commands are provided as below:

```sh
# assuming your Python alias is "python3"
# create the venv into folder ".venv"
python3 -m venv .venv
# activate the venv
source .venv/bin/activate
```

For other OS or details, please check [here](https://docs.python.org/3/library/venv.html).

### Install the packages

```sh
# assume your Python alias is "python" (if you have activated the venv, the alias should become "python")
python -m pip install -r requirements.txt
```

### Data files and output files
You can put your data files into `data/` and output files into `output/`. These two files are configured to be ignored by Git to reduce the overall repository size. You may need to create these two directories yourself.