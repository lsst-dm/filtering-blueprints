# RestrictedPython Exploration

## Viewing the notebook

### [RestrictedPython exploratory Jupyter notebook](explore-restricted-python.ipynb)

## Running the notebook

### Set up the Docker image

This image is based off the image lsstsqre/centos:7-stack-lsst_distrib-d_2018_07_09 and includes RestrictedPython.

```
$ docker build -t "respy" .
```

### Run the notebook in a container

Because of the way the LSST stack images are built, the default CMD for the image is /bin/bash and cannot be overridden by other command line arguments (e.g., passing "python" to enter a Python shell upon running the container) or else ```setup lsst_distrib``` will not be run.

To run the Jupyter notebook, first start the container:

```
$ docker run -it --rm \
      -v $PWD:/home/respy \
      -v $PWD:/home/jovyan/work \
      -p 8888:8888 \
      respy
```

From within the container run:

```
[lsst@respy]$ jupyter notebook --ip=0.0.0.0 --no-browser
```

The resulting URL will be in the format [uid]:8888/?token=sometoken.
You must replace the [uid] with "localhost" in your browser.
