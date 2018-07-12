# Working with RestrictedPython

Notebooks
---------

* [Exploring RestrictedPython](explore-restricted-python.ipynb)

To run notebooks
----------------

**Build Docker image from SQuARe's LSST Image** 

```
$ docker build -t "respy" .
```

**Run Docker image**

Because of the way lsst_distrib needs to be setup, the CMD command **should not** be overridden.  
/bin/bash must be used and is the default.  
All other commands need to be run inside the container.

```
$ docker run -it --rm \
	-v $PWD:/home/respy \
	-v $PWD:/home/jovyan/work \
	-p 8888:8888 \
	respy
```

**Run Jupyter inside the container**

```
[lsst@respy] jupyter notebook --no-browser --ip=0.0.0.0
```

The address for the notebook will be given as http://<uid>:8888/?token=sometoken. You must replace the <uid> with localhost in your browser.

