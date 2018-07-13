import numpy
import scipy
import pandas
import random
# Packages with subpackages must be imported descending from base
import sklearn
import sklearn.svm
import astropy
import astropy.io
import lsst
import lsst.afw
import lsst.afw.math
import lsst.afw.image
import badmodule

lsst_builtins = {}

lsst_builtins['numpy'] = numpy
lsst_builtins['scipy'] = scipy
lsst_builtins['pandas'] = pandas
lsst_builtins['random'] = random
lsst_builtins['sklearn'] = sklearn
lsst_builtins['sklearn.svm'] = sklearn.svm
lsst_builtins['astropy'] = astropy
lsst_builtins['astropy.io'] = astropy.io
lsst_builtins['lsst'] = lsst
lsst_builtins['lsst.afw'] = lsst.afw
lsst_builtins['lsst.afw.math'] = lsst.afw.math
lsst_builtins['lsst.afw.image'] = lsst.afw.image
lsst_builtins['badmodule'] = badmodule
