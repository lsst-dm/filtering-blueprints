FROM lsstsqre/centos:7-stack-lsst_distrib-d_2018_07_09
LABEL maintainer "maria.t.patterson@gmail.com"
ENV REFRESHED_AT 2018-07-11

USER root

RUN yum -y update
RUN yum -y install python-pip
RUN yum -y install python-devel

WORKDIR /home/gitrepos

# Pip installs.
RUN source /opt/lsst/software/stack/loadLSST.bash; \
    pip install -U pip setuptools && \
    pip install pandas && \
    pip install sklearn && \
    pip install jupyter && \
    git clone https://github.com/zopefoundation/RestrictedPython.git && \
    cd RestrictedPython && git checkout 4.0b4 && pip install .

RUN mkdir /home/respy
WORKDIR /home/respy

USER lsst

# Setup lsst_distrib
ADD run_setup.sh /home/setup/
ADD setup_profile /home/setup/

ENTRYPOINT ["/bin/bash","--"]
CMD ["/home/setup/run_setup.sh"]

# Equivalent to above setup
#RUN echo "source /opt/lsst/software/stack/loadLSST.bash" >> /etc/bashrc
#RUN echo "setup lsst_distrib" >> /etc/bashrc
#CMD ["/bin/bash"]
